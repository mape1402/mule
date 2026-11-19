namespace Mule.Dispatching;

using System.Threading.Channels;

internal sealed class MuleSchedulerSignal
{
    private readonly Channel<bool> _recoverySignals = CreateSignalChannel();
    private readonly Channel<bool> _cleanupSignals = CreateSignalChannel();
    private readonly Channel<bool> _replaySignals = CreateSignalChannel();

    public void SignalRecovery()
        => _recoverySignals.Writer.TryWrite(true);

    public void SignalCleanup()
        => _cleanupSignals.Writer.TryWrite(true);

    public void SignalRecoveryAt(DateTimeOffset dueOnUtc)
        => SignalAt(_recoverySignals, dueOnUtc);

    public void SignalCleanupAt(DateTimeOffset dueOnUtc)
        => SignalAt(_cleanupSignals, dueOnUtc);

    public void SignalReplayAt(DateTimeOffset dueOnUtc)
        => SignalAt(_replaySignals, dueOnUtc);

    public async ValueTask WaitForRecoveryAsync(CancellationToken cancellationToken)
        => await _recoverySignals.Reader.ReadAsync(cancellationToken);

    public async ValueTask WaitForCleanupAsync(CancellationToken cancellationToken)
        => await _cleanupSignals.Reader.ReadAsync(cancellationToken);

    public async ValueTask WaitForReplayAsync(CancellationToken cancellationToken)
        => await _replaySignals.Reader.ReadAsync(cancellationToken);

    private static void SignalAt(Channel<bool> channel, DateTimeOffset dueOnUtc)
        => channel.Writer.TryWrite(true);

    private static Channel<bool> CreateSignalChannel()
        => Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = false,
            SingleWriter = false
        });
}
