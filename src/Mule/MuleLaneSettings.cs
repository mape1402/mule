namespace Mule;

public sealed class MuleLaneSettings
{
    public int WorkerCount { get; set; }

    public int MaxDegreeOfParallelism { get; set; }

    public int DispatchBatchSize { get; set; }

    public int DispatchQueueCapacity { get; set; }

    public TimeSpan PollingInterval { get; set; }

    public int MaxAttempts { get; set; }

    public TimeSpan RetryDelay { get; set; }

    public MuleRetryPolicy RetryPolicy { get; set; }

    public int Priority { get; set; }
}
