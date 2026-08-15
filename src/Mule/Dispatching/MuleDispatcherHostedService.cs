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
    private readonly Dictionary<string, SemaphoreSlim> _laneSemaphores;
    private readonly object _laneSemaphoreGate = new();

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
        _laneSemaphores = GetLaneNames()
            .ToDictionary(
                lane => lane,
                lane => new SemaphoreSlim(GetMaxDegreeOfParallelism(lane)),
                StringComparer.OrdinalIgnoreCase);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queueWorkers = Enumerable
            .Range(0, GetWorkerCount(MuleSettings.DefaultLane))
            .Select(_ => ProcessQueuedActionsAsync(stoppingToken))
            .ToArray();
        var recoveryWorker = _settings.RecoveryMode == MuleRecoveryMode.Scheduled
            ? RecoverPendingActionsOnScheduleAsync(stoppingToken)
            : RecoverPendingActionsByPollingAsync(stoppingToken);
        var cleanupWorker = _settings.CleanupMode switch
        {
            MuleCleanupMode.Disabled => Task.CompletedTask,
            MuleCleanupMode.Scheduled => CleanCompletedOnScheduleAsync(stoppingToken),
            _ => CleanCompletedByPollingAsync(stoppingToken)
        };

        await Task.WhenAll(queueWorkers.Append(recoveryWorker).Append(cleanupWorker));
    }

    private async Task ProcessQueuedActionsAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var actionId = await _dispatchQueue.DequeueAsync(cancellationToken);
                await DispatchQueuedActionAsync(actionId, cancellationToken);
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
                await ClaimAndDispatchPendingActionsAsync(cancellationToken);
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
                await ClaimAndDispatchPendingActionsAsync(cancellationToken);
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

    private async Task ClaimAndDispatchPendingActionsAsync(CancellationToken cancellationToken)
    {
        var tasks = GetLaneNames()
            .SelectMany(lane => Enumerable
                .Range(0, GetWorkerCount(lane))
                .Select(_ => ClaimAndDispatchLaneAsync(lane, cancellationToken)))
            .ToArray();

        await Task.WhenAll(tasks);
    }

    private async Task ClaimAndDispatchLaneAsync(string lane, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var actions = await storage.ClaimPendingAsync(lane, GetDispatchBatchSize(lane), GetLockTimeout(), DateTimeOffset.UtcNow, cancellationToken);

        await Task.WhenAll(actions.Select(action => DispatchClaimedActionAsync(action, cancellationToken)));
    }

    private async Task<DateTimeOffset?> GetNextPendingOnUtcAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        return await storage.GetNextPendingOnUtcAsync(GetLockTimeout(), DateTimeOffset.UtcNow, cancellationToken);
    }

    private async Task DispatchQueuedActionAsync(Guid actionId, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var action = await storage.LockAsync(actionId, GetLockTimeout(), DateTimeOffset.UtcNow, cancellationToken);

        if (action == null)
            return;

        await storage.SaveChangesAsync(cancellationToken);

        await DispatchActionAsync(action, cancellationToken);
    }

    private async Task DispatchClaimedActionAsync(DurableAction action, CancellationToken cancellationToken)
    {
        if (action == null)
            return;

        await DispatchActionAsync(action, cancellationToken);
    }

    private async Task DispatchActionAsync(DurableAction action, CancellationToken cancellationToken)
    {
        var semaphore = GetLaneSemaphore(action.Lane);
        await semaphore.WaitAsync(cancellationToken);

        try
        {
            await ExecuteActionAsync(action, cancellationToken);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task ExecuteActionAsync(DurableAction action, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
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
            nextAttemptOnUtc = action.Attempts + 1 >= GetMaxAttempts(action.Lane)
                ? null
                : DateTimeOffset.UtcNow.Add(GetRetryDelay(action.Lane));

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

    private IReadOnlyCollection<string> GetLaneNames()
        => new[] { MuleSettings.DefaultLane }
            .Concat(_settings.Lanes.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderByDescending(GetPriority)
            .ThenBy(x => x, StringComparer.OrdinalIgnoreCase)
            .ToArray();

    private MuleLaneSettings GetLaneSettings(string lane)
        => _settings.Lanes.TryGetValue(NormalizeLane(lane), out var settings) ? settings : null;

    private SemaphoreSlim GetLaneSemaphore(string lane)
    {
        lane = NormalizeLane(lane);
        lock (_laneSemaphoreGate)
        {
            if (_laneSemaphores.TryGetValue(lane, out var semaphore))
                return semaphore;

            semaphore = new SemaphoreSlim(GetMaxDegreeOfParallelism(lane));
            _laneSemaphores[lane] = semaphore;
            return semaphore;
        }
    }

    private int GetWorkerCount(string lane)
        => Math.Max(1, GetLaneSettings(lane)?.WorkerCount ?? _settings.WorkerCount);

    private int GetMaxDegreeOfParallelism(string lane)
        => Math.Max(1, GetLaneSettings(lane)?.MaxDegreeOfParallelism ?? _settings.MaxDegreeOfParallelism);

    private int GetDispatchBatchSize(string lane)
        => Math.Max(1, GetLaneSettings(lane)?.DispatchBatchSize ?? _settings.DispatchBatchSize);

    private int GetPriority(string lane)
        => GetLaneSettings(lane)?.Priority ?? 0;

    private int GetCleanupBatchSize()
        => Math.Max(1, _settings.CleanupBatchSize);

    private int GetMaxAttempts(string lane)
        => Math.Max(1, GetLaneSettings(lane)?.MaxAttempts ?? _settings.MaxAttempts);

    private TimeSpan GetRetryDelay(string lane)
    {
        var retryDelay = GetLaneSettings(lane)?.RetryDelay ?? _settings.RetryDelay;
        return retryDelay <= TimeSpan.Zero ? TimeSpan.FromSeconds(30) : retryDelay;
    }

    private TimeSpan GetLockTimeout()
        => _settings.LockTimeout <= TimeSpan.Zero ? TimeSpan.FromMinutes(5) : _settings.LockTimeout;

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;
}
