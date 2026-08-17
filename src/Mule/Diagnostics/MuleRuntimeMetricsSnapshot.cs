namespace Mule.Diagnostics;

public sealed record MuleRuntimeMetricsSnapshot(
    int Completed,
    int Failed,
    int DuplicatesIgnored,
    int ThroughputPerMinute,
    IReadOnlyDictionary<string, int> CompletedPerMinuteByLane,
    IReadOnlyDictionary<ActionKey, int> CompletedPerMinuteByActionKey,
    IReadOnlyDictionary<string, int> RuntimeFailedByLane,
    IReadOnlyDictionary<ActionKey, int> RuntimeFailedByActionKey,
    IReadOnlyDictionary<string, int> ClaimedByLane,
    IReadOnlyDictionary<string, int> WaitingExecutionByLane,
    IReadOnlyDictionary<string, int> ExecutingByLane,
    IReadOnlyDictionary<string, int> ExecutorSaturationByLane,
    IReadOnlyDictionary<string, TimeSpan?> OldestWaitingExecutionAgeByLane);
