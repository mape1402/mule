namespace Mule;

public sealed class MuleLaneSettings
{
    public int WorkerCount { get; set; }

    public int MaxDegreeOfParallelism { get; set; }

    public int DispatchBatchSize { get; set; }

    public int MaxDrainBatchesPerCycle { get; set; }

    public int MaxDrainActionsPerCycle { get; set; }

    public bool? DrainUntilEmpty { get; set; }

    public TimeSpan YieldBetweenDrainBatches { get; set; }

    public int DispatchQueueCapacity { get; set; }

    public TimeSpan PollingInterval { get; set; }

    public int MaxAttempts { get; set; }

    public TimeSpan RetryDelay { get; set; }

    public MuleRetryPolicy RetryPolicy { get; set; }

    public int Priority { get; set; }

    public int Weight { get; set; }
}
