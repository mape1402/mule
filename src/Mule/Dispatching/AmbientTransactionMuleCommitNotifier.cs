namespace Mule.Dispatching;

using System.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

internal sealed class AmbientTransactionMuleCommitNotifier : IMuleCommitNotifier
{
    private readonly IMuleDispatchQueue _dispatchQueue;
    private readonly MuleSettings _settings;
    private readonly ILogger<AmbientTransactionMuleCommitNotifier> _logger;

    public AmbientTransactionMuleCommitNotifier(
        IMuleDispatchQueue dispatchQueue,
        IOptions<MuleSettings> settings,
        ILogger<AmbientTransactionMuleCommitNotifier> logger = null)
    {
        _dispatchQueue = dispatchQueue ?? throw new ArgumentNullException(nameof(dispatchQueue));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
        _logger = logger ?? NullLogger<AmbientTransactionMuleCommitNotifier>.Instance;
    }

    public async ValueTask NotifySavedAsync(Guid actionId, CancellationToken cancellationToken = default)
    {
        if (!_settings.ImmediateDispatch)
            return;

        var transaction = Transaction.Current;

        if (transaction == null)
        {
            await _dispatchQueue.EnqueueAsync(actionId, cancellationToken);
            return;
        }

        transaction.TransactionCompleted += (_, args) =>
        {
            if (args.Transaction.TransactionInformation.Status != TransactionStatus.Committed)
                return;

            ThreadPool.QueueUserWorkItem(_ => _ = EnqueueCommittedActionAsync(actionId));
        };
    }

    private async Task EnqueueCommittedActionAsync(Guid actionId)
    {
        try
        {
            await _dispatchQueue.EnqueueAsync(actionId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Mule action {MuleActionId} could not be queued after transaction commit.", actionId);
        }
    }
}
