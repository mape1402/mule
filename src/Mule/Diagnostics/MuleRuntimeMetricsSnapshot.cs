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
    IReadOnlyDictionary<string, TimeSpan?> OldestWaitingExecutionAgeByLane)
{
    public int ReplayScheduled { get; init; }

    public int ReplayMaterialized { get; init; }

    public int ReplayAlreadyScheduled { get; init; }

    public int ReplayRejected { get; init; }

    public int ReplayRetried { get; init; }

    public int ReplayMaterializationFailures { get; init; }

    public IReadOnlyDictionary<string, int> DeferredByLane { get; init; } = new Dictionary<string, int>();

    public IReadOnlyDictionary<DeferralReason, int> DeferredByReason { get; init; } = new Dictionary<DeferralReason, int>();

    public IReadOnlyDictionary<string, int> ExecutionKeyContentionByLane { get; init; } = new Dictionary<string, int>();

    public int ActivationsOmitted { get; init; }

    public int ActivationsRecovered { get; init; }
}
