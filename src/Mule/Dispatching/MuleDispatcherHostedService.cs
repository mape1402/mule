namespace Mule.Dispatching;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

internal sealed class MuleDispatcherHostedService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IMuleDispatchQueue _dispatchQueue;
    private readonly MuleSettings _settings;
    private readonly ILogger<MuleDispatcherHostedService> _logger;
    private DateTimeOffset _nextCleanupOnUtc = DateTimeOffset.MinValue;

    public MuleDispatcherHostedService(
        IServiceScopeFactory scopeFactory,
        IMuleDispatchQueue dispatchQueue,
        IOptions<MuleSettings> settings,
        ILogger<MuleDispatcherHostedService> logger)
    {
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _dispatchQueue = dispatchQueue ?? throw new ArgumentNullException(nameof(dispatchQueue));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queueWorker = ProcessQueuedActionsAsync(stoppingToken);
        var recoveryWorker = RecoverPendingActionsAsync(stoppingToken);

        await Task.WhenAll(queueWorker, recoveryWorker);
    }

    private async Task ProcessQueuedActionsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var actionId = await _dispatchQueue.DequeueAsync(cancellationToken);
                await DispatchActionAsync(actionId, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mule queue dispatcher failed.");
            }
        }
    }

    private async Task RecoverPendingActionsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await QueuePendingActionsAsync(cancellationToken);
                await CleanCompletedAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mule recovery dispatcher failed.");
            }

            await Task.Delay(GetDispatchInterval(), cancellationToken);
        }
    }

    private async Task QueuePendingActionsAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var actions = await storage.LockPendingAsync(GetDispatchBatchSize(), GetLockTimeout(), DateTimeOffset.UtcNow, cancellationToken);

        foreach (var action in actions)
            await _dispatchQueue.EnqueueAsync(action.Id, cancellationToken);
    }

    private async Task DispatchActionAsync(Guid actionId, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var action = await storage.LockAsync(actionId, GetLockTimeout(), DateTimeOffset.UtcNow, cancellationToken);

        if (action == null)
            return;

        await storage.SaveChangesAsync(cancellationToken);

        var registry = scope.ServiceProvider.GetRequiredService<MuleActionRegistry>();

        try
        {
            if (!registry.TryGetHandler(action.Key, out var handler))
                throw new InvalidOperationException($"No Mule handler is registered for action key '{action.Key}'.");

            await handler.ExecuteAsync(action, scope.ServiceProvider, cancellationToken);
            await storage.MarkCompletedAsync(action.Id, DateTimeOffset.UtcNow, cancellationToken);
        }
        catch (Exception ex)
        {
            var nextAttempt = action.Attempts + 1 >= GetMaxAttempts()
                ? (DateTimeOffset?)null
                : DateTimeOffset.UtcNow.Add(GetRetryDelay());

            await storage.MarkFailedAsync(action.Id, ex.ToString(), DateTimeOffset.UtcNow, nextAttempt, cancellationToken);
            _logger.LogError(ex, "Mule action {MuleActionId} failed.", action.Id);
        }

        await storage.SaveChangesAsync(cancellationToken);
    }

    private async Task CleanCompletedAsync(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;

        if (now < _nextCleanupOnUtc)
            return;

        _nextCleanupOnUtc = now.Add(GetCleanupInterval());

        using var scope = _scopeFactory.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var deleted = await storage.CleanCompletedAsync(now.Subtract(GetCompletedRetention()), GetCleanupBatchSize(), cancellationToken);

        if (deleted > 0)
            await storage.SaveChangesAsync(cancellationToken);
    }

    private TimeSpan GetDispatchInterval()
        => _settings.DispatchInterval <= TimeSpan.Zero ? TimeSpan.FromMinutes(1) : _settings.DispatchInterval;

    private TimeSpan GetCleanupInterval()
        => _settings.CleanupInterval <= TimeSpan.Zero ? TimeSpan.FromMinutes(10) : _settings.CleanupInterval;

    private TimeSpan GetCompletedRetention()
        => _settings.CompletedRetention <= TimeSpan.Zero ? TimeSpan.FromDays(1) : _settings.CompletedRetention;

    private int GetDispatchBatchSize()
        => Math.Max(1, _settings.DispatchBatchSize);

    private int GetCleanupBatchSize()
        => Math.Max(1, _settings.CleanupBatchSize);

    private int GetMaxAttempts()
        => Math.Max(1, _settings.MaxAttempts);

    private TimeSpan GetRetryDelay()
        => _settings.RetryDelay <= TimeSpan.Zero ? TimeSpan.FromSeconds(30) : _settings.RetryDelay;

    private TimeSpan GetLockTimeout()
        => _settings.LockTimeout <= TimeSpan.Zero ? TimeSpan.FromMinutes(5) : _settings.LockTimeout;
}
