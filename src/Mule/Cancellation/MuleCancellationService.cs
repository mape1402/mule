namespace Mule.Cancellation;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

internal sealed class MuleCancellationService : IMuleCancellationService
{
    private readonly IServiceProvider _services;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<MuleCancellationService> _logger;

    public MuleCancellationService(
        IServiceProvider services,
        TimeProvider timeProvider,
        ILogger<MuleCancellationService> logger)
    {
        _services = services ?? throw new ArgumentNullException(nameof(services));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async ValueTask<MuleCancellationResult> CancelAsync(
        Ulid actionId,
        CancellationToken cancellationToken = default)
    {
        if (actionId == Ulid.Empty)
            throw new ArgumentException("An action ID is required.", nameof(actionId));

        var storage = _services.GetService<IMuleCancellationStorage>();
        if (storage == null)
            return new MuleCancellationResult(actionId, MuleCancellationOutcome.NotSupported);

        var result = await storage.CancelAsync(actionId, _timeProvider.GetUtcNow(), cancellationToken);
        LogResult(result);
        return result;
    }

    private void LogResult(MuleCancellationResult result)
    {
        if (result.Outcome == MuleCancellationOutcome.Cancelled)
        {
            _logger.LogInformation("Mule action {MuleActionId} was cancelled.", result.ActionId);
            return;
        }

        if (result.Outcome == MuleCancellationOutcome.AlreadyCancelled)
        {
            _logger.LogInformation("Mule action {MuleActionId} was already cancelled.", result.ActionId);
            return;
        }

        _logger.LogWarning(
            "Mule action {MuleActionId} could not be cancelled with outcome {MuleCancellationOutcome}.",
            result.ActionId,
            result.Outcome);
    }
}
