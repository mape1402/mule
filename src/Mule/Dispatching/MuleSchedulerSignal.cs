namespace Mule.Dispatching;

using System.Threading.Channels;

internal sealed class MuleSchedulerSignal
{
    private readonly Channel<bool> _recoverySignals = Channel.CreateUnbounded<bool>();
    private readonly Channel<bool> _cleanupSignals = Channel.CreateUnbounded<bool>();

    public void SignalRecovery()
        => _recoverySignals.Writer.TryWrite(true);

    public void SignalCleanup()
        => _cleanupSignals.Writer.TryWrite(true);

    public void SignalRecoveryAt(DateTimeOffset dueOnUtc)
        => SignalAt(_recoverySignals, dueOnUtc);

    public void SignalCleanupAt(DateTimeOffset dueOnUtc)
        => SignalAt(_cleanupSignals, dueOnUtc);

    public async ValueTask WaitForRecoveryAsync(CancellationToken cancellationToken)
        => await _recoverySignals.Reader.ReadAsync(cancellationToken);

    public async ValueTask WaitForCleanupAsync(CancellationToken cancellationToken)
        => await _cleanupSignals.Reader.ReadAsync(cancellationToken);

    private static void SignalAt(Channel<bool> channel, DateTimeOffset dueOnUtc)
    {
        _ = Task.Run(async () =>
        {
            var delay = dueOnUtc - DateTimeOffset.UtcNow;

            if (delay > TimeSpan.Zero)
                await Task.Delay(delay);

            channel.Writer.TryWrite(true);
        });
    }
}
