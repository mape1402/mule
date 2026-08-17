namespace Mule.Dispatching;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Mule.Diagnostics;

internal sealed class MuleDispatcherHostedService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IMuleDispatchQueue _dispatchQueue;
    private readonly MuleActionExecutor _executor;
    private readonly MuleSchedulerSignal _schedulerSignal;
    private readonly MuleSettings _settings;
    private readonly MuleRuntimeMetrics _metrics;
    private readonly ILogger<MuleDispatcherHostedService> _logger;
    private readonly Dictionary<string, DateTimeOffset> _nextLanePollingOnUtc = new(StringComparer.OrdinalIgnoreCase);
    private int _fairScheduleCursor;

    public MuleDispatcherHostedService(
        IServiceScopeFactory scopeFactory,
        IMuleDispatchQueue dispatchQueue,
        MuleActionExecutor executor,
        MuleSchedulerSignal schedulerSignal,
        IOptions<MuleSettings> settings,
        MuleRuntimeMetrics metrics,
        ILogger<MuleDispatcherHostedService> logger)
    {
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _dispatchQueue = dispatchQueue ?? throw new ArgumentNullException(nameof(dispatchQueue));
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        _schedulerSignal = schedulerSignal ?? throw new ArgumentNullException(nameof(schedulerSignal));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var assignedLanes = GetLaneNames().ToArray();
        var queueWorkers = assignedLanes
            .SelectMany(lane => Enumerable
                .Range(0, GetWorkerCount(lane))
                .Select(_ => ProcessQueuedActionsAsync(lane, stoppingToken)))
            .Concat(Enumerable
                .Range(0, GetWorkerCount(MuleSettings.DefaultLane))
                .Select(_ => ProcessUnassignedQueuedActionsAsync(assignedLanes, stoppingToken)))
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
        _executor.Start(stoppingToken);

        await Task.WhenAll(queueWorkers
            .Append(recoveryWorker)
            .Append(cleanupWorker));
    }

    private async Task ProcessQueuedActionsAsync(string lane, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var item = await _dispatchQueue.DequeueAsync(lane, cancellationToken);
                await DispatchQueuedActionAsync(item, cancellationToken);
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

    private async Task ProcessUnassignedQueuedActionsAsync(
        IReadOnlyCollection<string> assignedLanes,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var item = await _dispatchQueue.DequeueUnassignedAsync(assignedLanes, cancellationToken);
                await DispatchQueuedActionAsync(item, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mule fallback queue dispatcher failed.");
            }
        }
    }

    private async Task RecoverPendingActionsByPollingAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var now = DateTimeOffset.UtcNow;
                await ClaimAndDispatchDuePendingActionsAsync(now, cancellationToken);
                await DelayUntilNextLanePollingAsync(now, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mule recovery dispatcher failed.");
                await Task.Delay(GetDispatchInterval(), cancellationToken);
            }
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
        await ClaimAndDispatchLanesAsync(GetLaneNames(), cancellationToken);
    }

    private async Task ClaimAndDispatchDuePendingActionsAsync(DateTimeOffset now, CancellationToken cancellationToken)
    {
        var lanes = GetLaneNames()
            .Where(lane => IsLaneDueForPolling(lane, now))
            .ToArray();

        await ClaimAndDispatchLanesAsync(lanes, cancellationToken);

        foreach (var lane in lanes)
            _nextLanePollingOnUtc[NormalizeLane(lane)] = now.Add(GetPollingInterval(lane));
    }

    private async Task ClaimAndDispatchLanesAsync(IEnumerable<string> lanes, CancellationToken cancellationToken)
    {
        var selectedLanes = new HashSet<string>(
            lanes.Select(NormalizeLane),
            StringComparer.OrdinalIgnoreCase);

        var tasks = GetFairLaneNames()
            .Where(selectedLanes.Contains)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .SelectMany(lane => Enumerable
                .Range(0, GetWorkerCount(lane))
                .Select(_ => ClaimAndDispatchLaneAsync(lane, cancellationToken)))
            .ToArray();

        if (tasks.Length > 0)
            await Task.WhenAll(tasks);
    }

    private bool IsLaneDueForPolling(string lane, DateTimeOffset now)
        => !_nextLanePollingOnUtc.TryGetValue(NormalizeLane(lane), out var dueOnUtc) || dueOnUtc <= now;

    private async Task DelayUntilNextLanePollingAsync(DateTimeOffset now, CancellationToken cancellationToken)
    {
        var nextDueOnUtc = GetLaneNames()
            .Select(lane => _nextLanePollingOnUtc.TryGetValue(NormalizeLane(lane), out var dueOnUtc)
                ? dueOnUtc
                : now)
            .OrderBy(x => x)
            .FirstOrDefault();

        var delay = nextDueOnUtc - DateTimeOffset.UtcNow;
        if (delay <= TimeSpan.Zero)
            return;

        await Task.Delay(delay, cancellationToken);
    }

    private async Task ClaimAndDispatchLaneAsync(string lane, CancellationToken cancellationToken)
    {
        var batches = 0;
        var actionsClaimed = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            if (batches >= GetMaxDrainBatchesPerCycle(lane))
                return;

            var remainingActions = GetRemainingDrainActions(lane, actionsClaimed);
            if (remainingActions == 0)
                return;

            using var scope = _scopeFactory.CreateScope();
            var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
            var batchSize = remainingActions == null
                ? GetDispatchBatchSize(lane)
                : Math.Min(GetDispatchBatchSize(lane), remainingActions.Value);
            await _executor.WaitForCapacityAsync(lane, cancellationToken);
            var actions = await storage.ClaimPendingAsync(lane, batchSize, GetLockTimeout(), DateTimeOffset.UtcNow, cancellationToken);

            if (actions.Count == 0)
                return;

            batches++;
            actionsClaimed += actions.Count;
            _metrics.RecordClaimed(lane, actions.Count);

            foreach (var action in actions)
                await _executor.EnqueueAsync(action, cancellationToken);

            if (!GetDrainUntilEmpty(lane))
                return;

            var yield = GetYieldBetweenDrainBatches(lane);
            if (yield > TimeSpan.Zero)
                await Task.Delay(yield, cancellationToken);
            else
                await Task.Yield();
        }
    }

    private async Task<DateTimeOffset?> GetNextPendingOnUtcAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        return await storage.GetNextPendingOnUtcAsync(GetLockTimeout(), DateTimeOffset.UtcNow, cancellationToken);
    }

    private async Task DispatchQueuedActionAsync(MuleDispatchItem item, CancellationToken cancellationToken)
    {
        await _executor.WaitForCapacityAsync(item.Lane, cancellationToken);

        using var scope = _scopeFactory.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var action = await storage.LockAsync(item.ActionId, GetLockTimeout(), DateTimeOffset.UtcNow, cancellationToken);

        if (action == null)
            return;

        await storage.SaveChangesAsync(cancellationToken);
        _metrics.RecordClaimed(action.Lane, 1);

        await _executor.EnqueueAsync(action, cancellationToken);
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

    private IReadOnlyCollection<string> GetFairLaneNames()
    {
        var lanes = GetLaneNames();
        var schedule = lanes
            .SelectMany(lane => Enumerable.Repeat(lane, GetWeight(lane)))
            .ToArray();

        if (schedule.Length == 0)
            return lanes;

        var start = Math.Abs(Interlocked.Increment(ref _fairScheduleCursor) % schedule.Length);
        return schedule
            .Skip(start)
            .Concat(schedule.Take(start))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private MuleLaneSettings GetLaneSettings(string lane)
        => _settings.Lanes.TryGetValue(NormalizeLane(lane), out var settings) ? settings : null;

    private int GetWorkerCount(string lane)
    {
        var laneValue = GetLaneSettings(lane)?.WorkerCount;
        return Math.Max(1, laneValue > 0 ? laneValue.Value : _settings.WorkerCount);
    }

    private int GetDispatchBatchSize(string lane)
    {
        var laneValue = GetLaneSettings(lane)?.DispatchBatchSize;
        return Math.Max(1, laneValue > 0 ? laneValue.Value : _settings.DispatchBatchSize);
    }

    private int GetPriority(string lane)
        => GetLaneSettings(lane)?.Priority ?? 0;

    private int GetWeight(string lane)
    {
        var settings = GetLaneSettings(lane);
        var weight = settings?.Weight > 0 ? settings.Weight : Math.Max(1, settings?.Priority ?? 1);
        return Math.Clamp(weight, 1, 100);
    }

    private int GetMaxDrainBatchesPerCycle(string lane)
    {
        var laneValue = GetLaneSettings(lane)?.MaxDrainBatchesPerCycle;
        var configured = laneValue > 0 ? laneValue.Value : _settings.MaxDrainBatchesPerCycle;

        if (GetDrainUntilEmpty(lane) && configured <= 1)
            return int.MaxValue;

        return Math.Max(1, configured);
    }

    private int? GetRemainingDrainActions(string lane, int actionsClaimed)
    {
        var laneValue = GetLaneSettings(lane)?.MaxDrainActionsPerCycle;
        var maxActions = laneValue > 0 ? laneValue.Value : _settings.MaxDrainActionsPerCycle;

        if (maxActions <= 0)
            return null;

        return Math.Max(0, maxActions - actionsClaimed);
    }

    private bool GetDrainUntilEmpty(string lane)
        => GetLaneSettings(lane)?.DrainUntilEmpty ?? _settings.DrainUntilEmpty;

    private TimeSpan GetYieldBetweenDrainBatches(string lane)
    {
        var laneValue = GetLaneSettings(lane)?.YieldBetweenDrainBatches;
        return laneValue > TimeSpan.Zero ? laneValue.Value : _settings.YieldBetweenDrainBatches;
    }

    private TimeSpan GetPollingInterval(string lane)
    {
        var laneInterval = GetLaneSettings(lane)?.PollingInterval;
        var interval = laneInterval > TimeSpan.Zero ? laneInterval.Value : _settings.DispatchInterval;
        return interval <= TimeSpan.Zero ? TimeSpan.FromMinutes(1) : interval;
    }

    private int GetCleanupBatchSize()
        => Math.Max(1, _settings.CleanupBatchSize);

    private TimeSpan GetLockTimeout()
        => _settings.LockTimeout <= TimeSpan.Zero ? TimeSpan.FromMinutes(5) : _settings.LockTimeout;

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;
}
