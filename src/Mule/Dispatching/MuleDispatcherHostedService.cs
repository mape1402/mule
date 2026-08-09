namespace Mule.Dispatching;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

internal sealed class MuleDispatcherHostedService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IMuleDispatchQueue _dispatchQueue;
    private readonly MuleSchedulerSignal _schedulerSignal;
    private readonly MuleSettings _settings;
    private readonly ILogger<MuleDispatcherHostedService> _logger;

    public MuleDispatcherHostedService(
        IServiceScopeFactory scopeFactory,
        IMuleDispatchQueue dispatchQueue,
        MuleSchedulerSignal schedulerSignal,
        IOptions<MuleSettings> settings,
        ILogger<MuleDispatcherHostedService> logger)
    {
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _dispatchQueue = dispatchQueue ?? throw new ArgumentNullException(nameof(dispatchQueue));
        _schedulerSignal = schedulerSignal ?? throw new ArgumentNullException(nameof(schedulerSignal));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queueWorker = ProcessQueuedActionsAsync(stoppingToken);
        var recoveryWorker = _settings.RecoveryMode == MuleRecoveryMode.Scheduled
            ? RecoverPendingActionsOnScheduleAsync(stoppingToken)
            : RecoverPendingActionsByPollingAsync(stoppingToken);
        var cleanupWorker = _settings.CleanupMode switch
        {
            MuleCleanupMode.Disabled => Task.CompletedTask,
            MuleCleanupMode.Scheduled => CleanCompletedOnScheduleAsync(stoppingToken),
            _ => CleanCompletedByPollingAsync(stoppingToken)
        };

        await Task.WhenAll(queueWorker, recoveryWorker, cleanupWorker);
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

    private async Task RecoverPendingActionsByPollingAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await QueuePendingActionsAsync(cancellationToken);
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

    private async Task RecoverPendingActionsOnScheduleAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await QueuePendingActionsAsync(cancellationToken);
                var nextPendingOnUtc = await GetNextPendingOnUtcAsync(cancellationToken);
                await WaitForRecoveryAsync(nextPendingOnUtc, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mule scheduled recovery dispatcher failed.");
                await Task.Delay(GetDispatchInterval(), cancellationToken);
            }
        }
    }

    private async Task CleanCompletedByPollingAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await CleanCompletedAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mule cleanup dispatcher failed.");
            }

            await Task.Delay(GetCleanupInterval(), cancellationToken);
        }
    }

    private async Task CleanCompletedOnScheduleAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await CleanCompletedAsync(cancellationToken);
                var nextCleanupOnUtc = await GetNextCleanupOnUtcAsync(cancellationToken);
                await WaitForCleanupAsync(nextCleanupOnUtc, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mule scheduled cleanup dispatcher failed.");
                await Task.Delay(GetCleanupInterval(), cancellationToken);
            }
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

    private async Task<DateTimeOffset?> GetNextPendingOnUtcAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        return await storage.GetNextPendingOnUtcAsync(GetLockTimeout(), DateTimeOffset.UtcNow, cancellationToken);
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
        DateTimeOffset? nextAttemptOnUtc = null;
        DateTimeOffset? completedOnUtc = null;

        try
        {
            if (!registry.TryGetHandler(action.Key, out var handler))
                throw new InvalidOperationException($"No Mule handler is registered for action key '{action.Key}'.");

            await handler.ExecuteAsync(action, scope.ServiceProvider, cancellationToken);
            completedOnUtc = DateTimeOffset.UtcNow;
            await storage.MarkCompletedAsync(action.Id, completedOnUtc.Value, cancellationToken);
        }
        catch (Exception ex)
        {
            nextAttemptOnUtc = action.Attempts + 1 >= GetMaxAttempts()
                ? null
                : DateTimeOffset.UtcNow.Add(GetRetryDelay());

            await storage.MarkFailedAsync(action.Id, ex.ToString(), DateTimeOffset.UtcNow, nextAttemptOnUtc, cancellationToken);
            _logger.LogError(ex, "Mule action {MuleActionId} failed.", action.Id);
        }

        await storage.SaveChangesAsync(cancellationToken);

        if (_settings.RecoveryMode == MuleRecoveryMode.Scheduled && nextAttemptOnUtc != null)
            _schedulerSignal.SignalRecoveryAt(nextAttemptOnUtc.Value);

        if (_settings.CleanupMode == MuleCleanupMode.Scheduled && completedOnUtc != null)
            _schedulerSignal.SignalCleanupAt(completedOnUtc.Value.Add(GetCompletedRetention()));
    }

    private async Task CleanCompletedAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var deleted = await storage.CleanCompletedAsync(
            DateTimeOffset.UtcNow.Subtract(GetCompletedRetention()),
            GetCleanupBatchSize(),
            cancellationToken);

        if (deleted > 0)
            await storage.SaveChangesAsync(cancellationToken);
    }

    private async Task<DateTimeOffset?> GetNextCleanupOnUtcAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var oldestCompletedOnUtc = await storage.GetOldestCompletedOnUtcAsync(cancellationToken);
        return oldestCompletedOnUtc?.Add(GetCompletedRetention());
    }

    private async Task WaitForRecoveryAsync(DateTimeOffset? dueOnUtc, CancellationToken cancellationToken)
    {
        if (dueOnUtc == null)
        {
            await _schedulerSignal.WaitForRecoveryAsync(cancellationToken);
            return;
        }

        await WaitForSignalOrDueAsync(
            token => _schedulerSignal.WaitForRecoveryAsync(token).AsTask(),
            dueOnUtc.Value,
            cancellationToken);
    }

    private async Task WaitForCleanupAsync(DateTimeOffset? dueOnUtc, CancellationToken cancellationToken)
    {
        if (dueOnUtc == null)
        {
            await _schedulerSignal.WaitForCleanupAsync(cancellationToken);
            return;
        }

        await WaitForSignalOrDueAsync(
            token => _schedulerSignal.WaitForCleanupAsync(token).AsTask(),
            dueOnUtc.Value,
            cancellationToken);
    }

    private static async Task WaitForSignalOrDueAsync(
        Func<CancellationToken, Task> waitForSignal,
        DateTimeOffset dueOnUtc,
        CancellationToken cancellationToken)
    {
        var delay = dueOnUtc - DateTimeOffset.UtcNow;

        if (delay <= TimeSpan.Zero)
            return;

        using var signalCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var signalTask = waitForSignal(signalCancellation.Token);
        var delayTask = Task.Delay(delay, cancellationToken);
        var completed = await Task.WhenAny(signalTask, delayTask);

        if (completed == signalTask)
            await signalTask;
        else
            await signalCancellation.CancelAsync();
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
