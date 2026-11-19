namespace Mule.Dispatching;

using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Mule.Diagnostics;

internal sealed class MuleActionExecutor : IMuleActionExecutor
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly MuleSchedulerSignal _schedulerSignal;
    private readonly TimeProvider _timeProvider;
    private readonly MuleSettings _settings;
    private readonly MuleRuntimeMetrics _metrics;
    private readonly ILogger<MuleActionExecutor> _logger;
    private readonly object _gate = new();
    private readonly Dictionary<string, LaneExecutor> _lanes = new(StringComparer.OrdinalIgnoreCase);
    private CancellationToken _stoppingToken;

    public MuleActionExecutor(
        IServiceScopeFactory scopeFactory,
        MuleSchedulerSignal schedulerSignal,
        IOptions<MuleSettings> settings,
        MuleRuntimeMetrics metrics,
        ILogger<MuleActionExecutor> logger,
        TimeProvider timeProvider = null)
    {
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _schedulerSignal = schedulerSignal ?? throw new ArgumentNullException(nameof(schedulerSignal));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public IReadOnlyCollection<Task> Start(CancellationToken stoppingToken)
    {
        _stoppingToken = stoppingToken;
        return GetLaneNames()
            .SelectMany(lane => GetLaneExecutor(lane).Workers)
            .ToArray();
    }

    public async ValueTask WaitForCapacityAsync(string lane, CancellationToken cancellationToken = default)
    {
        lane = NormalizeLane(lane);
        var executor = GetLaneExecutor(lane);

        if (!executor.IsBounded)
            return;

        var wait = executor.Channel.Writer.WaitToWriteAsync(cancellationToken);
        if (!wait.IsCompletedSuccessfully)
            _metrics.RecordExecutorSaturation(lane);

        await wait;
    }

    public async ValueTask EnqueueAsync(DurableAction action, CancellationToken cancellationToken = default)
    {
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        await EnqueueRangeAsync([action], cancellationToken);
    }

    internal ValueTask EnqueueAsync(
        DurableAction action,
        MuleExecutionKeyCoordinator.MuleExecutionKeyLease executionKeyLease,
        CancellationToken cancellationToken = default)
        => EnqueueCoreAsync(action, executionKeyLease, cancellationToken);

    public async ValueTask EnqueueRangeAsync(IReadOnlyCollection<DurableAction> actions, CancellationToken cancellationToken = default)
    {
        if (actions == null)
            throw new ArgumentNullException(nameof(actions));

        foreach (var action in actions)
        {
            if (action == null)
                throw new ArgumentException("Action collection cannot contain null values.", nameof(actions));

            await EnqueueCoreAsync(action, null, cancellationToken);
        }
    }

    private async ValueTask EnqueueCoreAsync(
        DurableAction action,
        MuleExecutionKeyCoordinator.MuleExecutionKeyLease executionKeyLease,
        CancellationToken cancellationToken)
    {
        var lane = NormalizeLane(action.Lane);
        var executor = GetLaneExecutor(lane);
        var writer = executor.Channel.Writer;
        var queuedOnUtc = DateTimeOffset.UtcNow;
        _metrics.RecordWaitingExecution(lane, queuedOnUtc);

        try
        {
            if (!writer.TryWrite(new ExecutionItem(action, queuedOnUtc, executionKeyLease)))
            {
                _metrics.RecordExecutorSaturation(lane);
                await writer.WriteAsync(new ExecutionItem(action, queuedOnUtc, executionKeyLease), cancellationToken);
            }
        }
        catch
        {
            _metrics.RecordWaitingExecutionCanceled(lane);
            executionKeyLease?.Dispose();
            throw;
        }
    }

    private LaneExecutor GetLaneExecutor(string lane)
    {
        lane = NormalizeLane(lane);
        lock (_gate)
        {
            if (_lanes.TryGetValue(lane, out var executor))
                return executor;

            executor = CreateLaneExecutor(lane);
            _lanes[lane] = executor;
            return executor;
        }
    }

    private LaneExecutor CreateLaneExecutor(string lane)
    {
        var capacity = GetExecutionQueueCapacity(lane);
        var channel = CreateChannel(capacity);
        var workers = Enumerable
            .Range(0, GetMaxDegreeOfParallelism(lane))
            .Select(_ => RunLaneWorkerAsync(lane, channel, _stoppingToken))
            .ToArray();

        return new LaneExecutor(channel, workers, capacity > 0);
    }

    private static Channel<ExecutionItem> CreateChannel(int capacity)
    {
        if (capacity > 0)
            return Channel.CreateBounded<ExecutionItem>(new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false
            });

        return Channel.CreateUnbounded<ExecutionItem>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = false
        });
    }

    private async Task RunLaneWorkerAsync(
        string lane,
        Channel<ExecutionItem> channel,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var executing = false;
            ExecutionItem item = null;
            try
            {
                item = await channel.Reader.ReadAsync(cancellationToken);
                executing = true;
                _metrics.RecordExecutionStarted(lane, item.QueuedOnUtc, DateTimeOffset.UtcNow);
                await ExecuteActionAsync(item.Action, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mule lane executor failed for lane {MuleLane}.", lane);
            }
            finally
            {
                if (executing)
                    _metrics.RecordExecutionStopped(lane);
                item?.ExecutionKeyLease?.Dispose();
            }
        }
    }

    private async Task ExecuteActionAsync(DurableAction action, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var registry = scope.ServiceProvider.GetRequiredService<MuleActionRegistry>();
        DateTimeOffset? nextAttemptOnUtc = null;
        DateTimeOffset? completedOnUtc = null;
        DateTimeOffset? deferredUntilUtc = null;
        MuleActionDeferral deferral;

        try
        {
            if (!registry.TryGetHandler(action.Key, out var handler))
                throw new InvalidOperationException($"No Mule handler is registered for action key '{action.Key}'.");

            deferral = await handler.ExecuteAsync(action, scope.ServiceProvider, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            var nextAttempt = action.Attempts + 1;
            var retryPolicy = GetRetryPolicy(action.Lane);
            var now = _timeProvider.GetUtcNow();
            nextAttemptOnUtc = nextAttempt >= retryPolicy.MaxAttempts
                ? null
                : now.Add(retryPolicy.GetDelay(nextAttempt));

            var applied = true;
            if (storage is IMuleExecutionStorage executionStorage)
            {
                var outcome = await executionStorage.FailClaimAsync(
                    action.Id,
                    action.ConcurrencyToken,
                    ex.ToString(),
                    now,
                    nextAttemptOnUtc,
                    cancellationToken);
                applied = outcome == MuleActionTransitionOutcome.Applied;
                if (!applied)
                    _logger.LogWarning(
                        "Mule action {MuleActionId} with claim {MuleClaimToken} lost its claim before failure persistence.",
                        action.Id,
                        action.ConcurrencyToken);
            }
            else
            {
                await storage.MarkFailedAsync(action.Id, ex.ToString(), now, nextAttemptOnUtc, cancellationToken);
                await storage.SaveChangesAsync(cancellationToken);
            }

            if (applied && nextAttemptOnUtc == null)
                _metrics.RecordFailed(action.Lane, action.Key);
            _logger.LogError(
                ex,
                "Mule action {MuleActionId} with claim {MuleClaimToken} failed in lane {MuleLane}.",
                action.Id,
                action.ConcurrencyToken,
                action.Lane);

            if (_settings.RecoveryMode == MuleRecoveryMode.Scheduled && nextAttemptOnUtc != null)
                _schedulerSignal.SignalRecoveryAt(nextAttemptOnUtc.Value);

            return;
        }

        if (deferral != null)
        {
            var now = _timeProvider.GetUtcNow();
            deferredUntilUtc = NormalizeDeferral(deferral.NotBeforeUtc, now);
            var executionStorage = RequireExecutionStorage(storage);
            var outcome = await executionStorage.DeferClaimAsync(
                action.Id,
                action.ConcurrencyToken,
                now,
                deferredUntilUtc.Value,
                deferral.Reason,
                cancellationToken);
            if (outcome == MuleActionTransitionOutcome.Applied)
            {
                _metrics.RecordDeferred(action.Lane, deferral.Reason);
                _logger.LogDebug(
                    "Deferred Mule action {MuleActionId} with claim {MuleClaimToken} in lane {MuleLane} until {MuleNotBeforeUtc} for {MuleDeferralReason}.",
                    action.Id,
                    action.ConcurrencyToken,
                    action.Lane,
                    deferredUntilUtc,
                    deferral.Reason.Value);
            }
            else
            {
                deferredUntilUtc = null;
                _logger.LogWarning(
                    "Mule action {MuleActionId} with claim {MuleClaimToken} lost its claim before deferral ({MuleTransitionOutcome}).",
                    action.Id,
                    action.ConcurrencyToken,
                    outcome);
            }
        }
        else
        {
            completedOnUtc = _timeProvider.GetUtcNow();
            if (storage is IMuleExecutionStorage executionStorage)
            {
                var outcome = await executionStorage.CompleteClaimAsync(
                    action.Id,
                    action.ConcurrencyToken,
                    completedOnUtc.Value,
                    cancellationToken);
                if (outcome != MuleActionTransitionOutcome.Applied)
                {
                    completedOnUtc = null;
                    _logger.LogWarning(
                        "Mule action {MuleActionId} with claim {MuleClaimToken} lost its claim before completion ({MuleTransitionOutcome}).",
                        action.Id,
                        action.ConcurrencyToken,
                        outcome);
                }
            }
            else
            {
                await storage.MarkCompletedAsync(action.Id, completedOnUtc.Value, cancellationToken);
                await storage.SaveChangesAsync(cancellationToken);
            }

            if (completedOnUtc != null)
                _metrics.RecordCompleted(action.Lane, action.Key, completedOnUtc.Value);
        }

        if (_settings.RecoveryMode == MuleRecoveryMode.Scheduled && deferredUntilUtc != null)
            _schedulerSignal.SignalRecoveryAt(deferredUntilUtc.Value);

        if (_settings.CleanupMode == MuleCleanupMode.Scheduled && completedOnUtc != null)
            _schedulerSignal.SignalCleanupAt(completedOnUtc.Value.Add(GetCompletedRetention()));
    }

    private IReadOnlyCollection<string> GetLaneNames()
        => new[] { MuleSettings.DefaultLane }
            .Concat(_settings.Lanes.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

    private int GetMaxDegreeOfParallelism(string lane)
    {
        var laneValue = GetLaneSettings(lane)?.MaxDegreeOfParallelism;
        return Math.Max(1, laneValue > 0 ? laneValue.Value : _settings.MaxDegreeOfParallelism);
    }

    private int GetExecutionQueueCapacity(string lane)
    {
        var laneValue = GetLaneSettings(lane)?.ExecutionQueueCapacity;
        return Math.Max(0, laneValue > 0 ? laneValue.Value : _settings.ExecutionQueueCapacity);
    }

    private MuleRetryPolicy GetRetryPolicy(string lane)
    {
        var laneSettings = GetLaneSettings(lane);
        var retryPolicy = laneSettings?.RetryPolicy ?? _settings.RetryPolicy;

        if (retryPolicy != null)
            return new MuleRetryPolicy
            {
                MaxAttempts = Math.Max(1, retryPolicy.MaxAttempts),
                Delay = retryPolicy.Delay <= TimeSpan.Zero ? TimeSpan.FromSeconds(30) : retryPolicy.Delay,
                MaxDelay = retryPolicy.MaxDelay,
                Backoff = retryPolicy.Backoff,
                JitterRatio = retryPolicy.JitterRatio
            };

        return new MuleRetryPolicy
        {
            MaxAttempts = Math.Max(1, laneSettings?.MaxAttempts > 0 ? laneSettings.MaxAttempts : _settings.MaxAttempts),
            Delay = GetLegacyRetryDelay(laneSettings)
        };
    }

    private TimeSpan GetLegacyRetryDelay(MuleLaneSettings laneSettings)
    {
        var retryDelay = laneSettings?.RetryDelay > TimeSpan.Zero ? laneSettings.RetryDelay : _settings.RetryDelay;
        return retryDelay <= TimeSpan.Zero ? TimeSpan.FromSeconds(30) : retryDelay;
    }

    private TimeSpan GetCompletedRetention()
        => _settings.CompletedRetention <= TimeSpan.Zero ? TimeSpan.FromDays(1) : _settings.CompletedRetention;

    private MuleLaneSettings GetLaneSettings(string lane)
        => _settings.Lanes.TryGetValue(NormalizeLane(lane), out var settings) ? settings : null;

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;

    private DateTimeOffset NormalizeDeferral(DateTimeOffset requested, DateTimeOffset now)
    {
        var minimumDelay = _settings.MinimumDeferralDelay <= TimeSpan.Zero
            ? TimeSpan.FromMilliseconds(100)
            : _settings.MinimumDeferralDelay;
        var minimum = now.Add(minimumDelay);
        return requested < minimum ? minimum : requested;
    }

    private static IMuleExecutionStorage RequireExecutionStorage(IMuleStorage storage)
        => storage as IMuleExecutionStorage
            ?? throw new NotSupportedException(
                $"Storage '{storage.GetType().FullName}' does not support cooperative deferral.");

    private sealed record ExecutionItem(
        DurableAction Action,
        DateTimeOffset QueuedOnUtc,
        MuleExecutionKeyCoordinator.MuleExecutionKeyLease ExecutionKeyLease);

    private sealed record LaneExecutor(Channel<ExecutionItem> Channel, IReadOnlyCollection<Task> Workers, bool IsBounded);
}
