namespace Mule.Dispatching;

using System.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Mule.Diagnostics;

internal sealed class AmbientTransactionMuleCommitNotifier : IMuleCommitNotifier
{
    private readonly IMuleDispatchQueue _dispatchQueue;
    private readonly MuleSchedulerSignal _schedulerSignal;
    private readonly MuleSettings _settings;
    private readonly ILogger<AmbientTransactionMuleCommitNotifier> _logger;
    private readonly MuleRuntimeMetrics _metrics;

    public AmbientTransactionMuleCommitNotifier(
        IMuleDispatchQueue dispatchQueue,
        MuleSchedulerSignal schedulerSignal,
        IOptions<MuleSettings> settings,
        ILogger<AmbientTransactionMuleCommitNotifier> logger = null,
        MuleRuntimeMetrics metrics = null)
    {
        _dispatchQueue = dispatchQueue ?? throw new ArgumentNullException(nameof(dispatchQueue));
        _schedulerSignal = schedulerSignal ?? throw new ArgumentNullException(nameof(schedulerSignal));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
        _logger = logger ?? NullLogger<AmbientTransactionMuleCommitNotifier>.Instance;
        _metrics = metrics ?? new MuleRuntimeMetrics();
    }

    public ValueTask NotifySavedAsync(Ulid actionId, string lane, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_settings.ImmediateDispatch)
        {
            if (_settings.RecoveryMode == MuleRecoveryMode.Scheduled)
                _schedulerSignal.SignalRecovery();

            return ValueTask.CompletedTask;
        }

        var transaction = Transaction.Current;

        if (transaction == null)
        {
            NotifyCommittedAction(actionId, lane);
            return ValueTask.CompletedTask;
        }

        transaction.TransactionCompleted += (_, args) =>
        {
            if (args.Transaction.TransactionInformation.Status != TransactionStatus.Committed)
                return;

            NotifyCommittedAction(actionId, lane);
        };

        return ValueTask.CompletedTask;
    }

    private void NotifyCommittedAction(Ulid actionId, string lane)
    {
        if (_dispatchQueue.TryEnqueue(actionId, lane))
            return;

        _metrics.RecordActivationOmitted();
        _schedulerSignal.SignalRecovery();
        _logger.LogDebug(
            "Mule action {MuleActionId} activation was coalesced because the immediate queue is full.",
            actionId);
    }
}
