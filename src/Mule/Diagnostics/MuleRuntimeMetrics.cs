namespace Mule.Diagnostics;

public sealed class MuleRuntimeMetrics
{
    private readonly object _gate = new();
    private readonly Queue<CompletedMetric> _completedOnUtc = new();
    private readonly Dictionary<string, int> _failedByLane = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<ActionKey, int> _failedByActionKey = new();
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
                new Dictionary<ActionKey, int>(_failedByActionKey));
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

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;

    private sealed record CompletedMetric(string Lane, ActionKey Key, DateTimeOffset CompletedOnUtc);
}
