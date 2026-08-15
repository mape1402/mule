namespace Mule;

public sealed class MuleLaneSettings
{
    public int WorkerCount { get; set; } = 1;

    public int MaxDegreeOfParallelism { get; set; } = 1;

    public int DispatchBatchSize { get; set; } = 50;

    public int DispatchQueueCapacity { get; set; }

    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMinutes(1);

    public int MaxAttempts { get; set; } = 10;

    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(30);

    public int Priority { get; set; }
}
