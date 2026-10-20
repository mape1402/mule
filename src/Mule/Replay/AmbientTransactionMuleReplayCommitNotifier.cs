namespace Mule.Replay;

using System.Transactions;
using Mule.Dispatching;

internal sealed class AmbientTransactionMuleReplayCommitNotifier : IMuleReplayCommitNotifier
{
    private readonly MuleSchedulerSignal _schedulerSignal;

    public AmbientTransactionMuleReplayCommitNotifier(MuleSchedulerSignal schedulerSignal)
        => _schedulerSignal = schedulerSignal ?? throw new ArgumentNullException(nameof(schedulerSignal));

    public ValueTask NotifyScheduledAsync(DateTimeOffset scheduleOnUtc)
    {
        var transaction = Transaction.Current;
        if (transaction == null)
        {
            _schedulerSignal.SignalReplayAt(scheduleOnUtc);
            return ValueTask.CompletedTask;
        }

        transaction.TransactionCompleted += (_, args) =>
        {
            if (args.Transaction.TransactionInformation.Status == TransactionStatus.Committed)
                _schedulerSignal.SignalReplayAt(scheduleOnUtc);
        };

        return ValueTask.CompletedTask;
    }
}
