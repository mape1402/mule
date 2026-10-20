namespace Mule;

public sealed class MuleReplaySettings
{
    public MuleRecoveryMode RecoveryMode { get; set; } = MuleRecoveryMode.Scheduled;

    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMinutes(1);

    public int BatchSize { get; set; } = 50;

    public int WorkerCount { get; set; } = 1;

    public TimeSpan LockTimeout { get; set; } = TimeSpan.FromMinutes(1);

    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(10);
}
