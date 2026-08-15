namespace Mule.Diagnostics;

public sealed class MuleDiagnosticsSnapshot
{
    public int Pending { get; init; }

    public int Locked { get; init; }

    public int Completed { get; init; }

    public int Failed { get; init; }

    public int LockExpired { get; init; }

    public int DuplicatesIgnored { get; init; }

    public int ThroughputPerMinute { get; init; }

    public int RuntimeCompleted { get; init; }

    public int RuntimeFailed { get; init; }

    public DateTimeOffset? OldestPendingOnUtc { get; init; }

    public DateTimeOffset? OldestLockedOnUtc { get; init; }

    public DateTimeOffset? OldestFailedOnUtc { get; init; }

    public TimeSpan? OldestPendingAge { get; init; }

    public TimeSpan? OldestLockedAge { get; init; }

    public TimeSpan? AverageEnqueueToExecutionLatency { get; init; }

    public TimeSpan? AverageExecutionLatency { get; init; }

    public TimeSpan? AverageEnqueueToTerminalLatency { get; init; }

    public IReadOnlyDictionary<string, int> BacklogByLane { get; init; } = new Dictionary<string, int>();

    public IReadOnlyDictionary<ActionKey, int> BacklogByActionKey { get; init; } = new Dictionary<ActionKey, int>();

    public IReadOnlyDictionary<ActionKey, int> FailuresByActionKey { get; init; } = new Dictionary<ActionKey, int>();

    public IReadOnlyDictionary<ActionKey, int> RetriesByActionKey { get; init; } = new Dictionary<ActionKey, int>();
}
