namespace Mule;

public sealed class MuleSettings
{
    public MuleRecoveryMode RecoveryMode { get; set; } = MuleRecoveryMode.Polling;

    public TimeSpan DispatchInterval { get; set; } = TimeSpan.FromMinutes(1);

    public bool ImmediateDispatch { get; set; } = true;

    public int DispatchQueueCapacity { get; set; }

    public MuleCleanupMode CleanupMode { get; set; } = MuleCleanupMode.Polling;

    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromMinutes(10);

    public TimeSpan CompletedRetention { get; set; } = TimeSpan.FromDays(1);

    public int DispatchBatchSize { get; set; } = 50;

    public int CleanupBatchSize { get; set; } = 500;

    public int MaxAttempts { get; set; } = 10;

    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan LockTimeout { get; set; } = TimeSpan.FromMinutes(5);
}
