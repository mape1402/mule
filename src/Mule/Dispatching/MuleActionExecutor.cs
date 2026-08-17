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
        ILogger<MuleActionExecutor> logger)
    {
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _schedulerSignal = schedulerSignal ?? throw new ArgumentNullException(nameof(schedulerSignal));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
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

        var lane = NormalizeLane(action.Lane);
        var executor = GetLaneExecutor(lane);
        var writer = executor.Channel.Writer;
        var queuedOnUtc = DateTimeOffset.UtcNow;
        _metrics.RecordWaitingExecution(lane, queuedOnUtc);

        try
        {
            if (!writer.TryWrite(new ExecutionItem(action, queuedOnUtc)))
            {
                _metrics.RecordExecutorSaturation(lane);
                await writer.WriteAsync(new ExecutionItem(action, queuedOnUtc), cancellationToken);
            }
        }
        catch
        {
            _metrics.RecordWaitingExecutionCanceled(lane);
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
            try
            {
                var item = await channel.Reader.ReadAsync(cancellationToken);
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

        try
        {
            if (!registry.TryGetHandler(action.Key, out var handler))
                throw new InvalidOperationException($"No Mule handler is registered for action key '{action.Key}'.");

            await handler.ExecuteAsync(action, scope.ServiceProvider, cancellationToken);
            completedOnUtc = DateTimeOffset.UtcNow;
            await storage.MarkCompletedAsync(action.Id, completedOnUtc.Value, cancellationToken);
            _metrics.RecordCompleted(action.Lane, action.Key, completedOnUtc.Value);
        }
        catch (Exception ex)
        {
            var nextAttempt = action.Attempts + 1;
            var retryPolicy = GetRetryPolicy(action.Lane);
            nextAttemptOnUtc = nextAttempt >= retryPolicy.MaxAttempts
                ? null
                : DateTimeOffset.UtcNow.Add(retryPolicy.GetDelay(nextAttempt));

            await storage.MarkFailedAsync(action.Id, ex.ToString(), DateTimeOffset.UtcNow, nextAttemptOnUtc, cancellationToken);
            if (nextAttemptOnUtc == null)
                _metrics.RecordFailed(action.Lane, action.Key);
            _logger.LogError(ex, "Mule action {MuleActionId} failed.", action.Id);
        }

        await storage.SaveChangesAsync(cancellationToken);

        if (_settings.RecoveryMode == MuleRecoveryMode.Scheduled && nextAttemptOnUtc != null)
            _schedulerSignal.SignalRecoveryAt(nextAttemptOnUtc.Value);

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

    private sealed record ExecutionItem(DurableAction Action, DateTimeOffset QueuedOnUtc);

    private sealed record LaneExecutor(Channel<ExecutionItem> Channel, IReadOnlyCollection<Task> Workers, bool IsBounded);
}
