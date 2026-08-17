namespace Mule.Diagnostics;

public sealed class MuleRuntimeMetrics
{
    private readonly object _gate = new();
    private readonly Queue<CompletedMetric> _completedOnUtc = new();
    private readonly Dictionary<string, int> _failedByLane = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<ActionKey, int> _failedByActionKey = new();
    private readonly Dictionary<string, int> _claimedByLane = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, int> _waitingExecutionByLane = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, int> _executingByLane = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, int> _executorSaturationByLane = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, Queue<DateTimeOffset>> _waitingExecutionOnUtcByLane = new(StringComparer.OrdinalIgnoreCase);
    private int _completed;
    private int _failed;
    private int _duplicatesIgnored;

    public void RecordCompleted(string lane, ActionKey key, DateTimeOffset completedOnUtc)
    {
        lock (_gate)
        {
            _completed++;
            _completedOnUtc.Enqueue(new CompletedMetric(NormalizeLane(lane), key, completedOnUtc));
            Trim(completedOnUtc);
        }
    }

    public void RecordFailed(string lane, ActionKey key)
    {
        lock (_gate)
        {
            _failed++;
            Increment(_failedByLane, NormalizeLane(lane));
            Increment(_failedByActionKey, key);
        }
    }

    public void RecordDuplicateIgnored()
    {
        lock (_gate)
            _duplicatesIgnored++;
    }

    public void RecordClaimed(string lane, int count)
    {
        if (count <= 0)
            return;

        lock (_gate)
            Add(_claimedByLane, NormalizeLane(lane), count);
    }

    public void RecordExecutorSaturation(string lane)
    {
        lock (_gate)
            Increment(_executorSaturationByLane, NormalizeLane(lane));
    }

    public void RecordWaitingExecution(string lane, DateTimeOffset queuedOnUtc)
    {
        lane = NormalizeLane(lane);
        lock (_gate)
        {
            Increment(_waitingExecutionByLane, lane);
            if (!_waitingExecutionOnUtcByLane.TryGetValue(lane, out var values))
            {
                values = new Queue<DateTimeOffset>();
                _waitingExecutionOnUtcByLane[lane] = values;
            }

            values.Enqueue(queuedOnUtc);
        }
    }

    public void RecordExecutionStarted(string lane, DateTimeOffset queuedOnUtc, DateTimeOffset now)
    {
        lane = NormalizeLane(lane);
        lock (_gate)
        {
            Decrement(_waitingExecutionByLane, lane);
            Increment(_executingByLane, lane);

            if (_waitingExecutionOnUtcByLane.TryGetValue(lane, out var values) && values.Count > 0)
                values.Dequeue();
        }
    }

    public void RecordWaitingExecutionCanceled(string lane)
    {
        lock (_gate)
            Decrement(_waitingExecutionByLane, NormalizeLane(lane));
    }

    public void RecordExecutionStopped(string lane)
    {
        lock (_gate)
            Decrement(_executingByLane, NormalizeLane(lane));
    }

    public MuleRuntimeMetricsSnapshot GetSnapshot(DateTimeOffset now)
    {
        lock (_gate)
        {
            Trim(now);
            return new MuleRuntimeMetricsSnapshot(
                _completed,
                _failed,
                _duplicatesIgnored,
                _completedOnUtc.Count,
                _completedOnUtc
                    .GroupBy(x => x.Lane, StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(x => x.Key, x => x.Count(), StringComparer.OrdinalIgnoreCase),
                _completedOnUtc
                    .GroupBy(x => x.Key)
                    .ToDictionary(x => x.Key, x => x.Count()),
                new Dictionary<string, int>(_failedByLane, StringComparer.OrdinalIgnoreCase),
                new Dictionary<ActionKey, int>(_failedByActionKey),
                new Dictionary<string, int>(_claimedByLane, StringComparer.OrdinalIgnoreCase),
                new Dictionary<string, int>(_waitingExecutionByLane, StringComparer.OrdinalIgnoreCase),
                new Dictionary<string, int>(_executingByLane, StringComparer.OrdinalIgnoreCase),
                new Dictionary<string, int>(_executorSaturationByLane, StringComparer.OrdinalIgnoreCase),
                _waitingExecutionOnUtcByLane.ToDictionary(
                    x => x.Key,
                    x => x.Value.Count == 0 ? null : (TimeSpan?)(now - x.Value.Peek()),
                    StringComparer.OrdinalIgnoreCase));
        }
    }

    private void Trim(DateTimeOffset now)
    {
        var oldest = now.AddMinutes(-1);

        while (_completedOnUtc.Count > 0 && _completedOnUtc.Peek().CompletedOnUtc < oldest)
            _completedOnUtc.Dequeue();
    }

    private static void Increment<TKey>(IDictionary<TKey, int> values, TKey key)
        => values[key] = values.TryGetValue(key, out var current) ? current + 1 : 1;

    private static void Add<TKey>(IDictionary<TKey, int> values, TKey key, int count)
        => values[key] = values.TryGetValue(key, out var current) ? current + count : count;

    private static void Decrement<TKey>(IDictionary<TKey, int> values, TKey key)
    {
        if (!values.TryGetValue(key, out var current) || current <= 1)
        {
            values.Remove(key);
            return;
        }

        values[key] = current - 1;
    }

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;

    private sealed record CompletedMetric(string Lane, ActionKey Key, DateTimeOffset CompletedOnUtc);
}
