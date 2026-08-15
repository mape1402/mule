namespace Mule.Diagnostics;

public sealed class MuleRuntimeMetrics
{
    private readonly object _gate = new();
    private readonly Queue<DateTimeOffset> _completedOnUtc = new();
    private int _completed;
    private int _failed;
    private int _duplicatesIgnored;

    public void RecordCompleted(DateTimeOffset completedOnUtc)
    {
        lock (_gate)
        {
            _completed++;
            _completedOnUtc.Enqueue(completedOnUtc);
            Trim(completedOnUtc);
        }
    }

    public void RecordFailed()
    {
        lock (_gate)
            _failed++;
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
                _completedOnUtc.Count);
        }
    }

    private void Trim(DateTimeOffset now)
    {
        var oldest = now.AddMinutes(-1);

        while (_completedOnUtc.Count > 0 && _completedOnUtc.Peek() < oldest)
            _completedOnUtc.Dequeue();
    }
}
