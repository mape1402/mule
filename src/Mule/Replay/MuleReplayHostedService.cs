namespace Mule.Replay;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Mule.Dispatching;

internal sealed class MuleReplayHostedService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IMuleCommitNotifier _commitNotifier;
    private readonly MuleSchedulerSignal _schedulerSignal;
    private readonly MuleReplaySettings _settings;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<MuleReplayHostedService> _logger;

    public MuleReplayHostedService(
        IServiceScopeFactory scopeFactory,
        IMuleCommitNotifier commitNotifier,
        MuleSchedulerSignal schedulerSignal,
        IOptions<MuleSettings> settings,
        TimeProvider timeProvider,
        ILogger<MuleReplayHostedService> logger)
    {
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _commitNotifier = commitNotifier ?? throw new ArgumentNullException(nameof(commitNotifier));
        _schedulerSignal = schedulerSignal ?? throw new ArgumentNullException(nameof(schedulerSignal));
        _settings = settings?.Value?.Replay ?? throw new ArgumentNullException(nameof(settings));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => _settings.RecoveryMode == MuleRecoveryMode.Polling
            ? RunByPollingAsync(stoppingToken)
            : RunOnScheduleAsync(stoppingToken);

    private async Task RunByPollingAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await ProcessSafelyAsync(cancellationToken);
            await Task.Delay(GetPollingInterval(), cancellationToken);
        }
    }

    private async Task RunOnScheduleAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await ProcessSafelyAsync(cancellationToken);
            var next = await GetNextScheduledOnUtcAsync(cancellationToken);
            await WaitForSignalOrDueAsync(next, cancellationToken);
        }
    }

    private async Task ProcessSafelyAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await ProcessBatchAsync(cancellationToken) >= GetBatchSize())
                await Task.Yield();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Mule replay materialization cycle failed.");
            await Task.Delay(GetRetryDelay(), cancellationToken);
        }
    }

    private async Task<int> ProcessBatchAsync(CancellationToken cancellationToken)
    {
        IReadOnlyCollection<MuleReplayClaim> claims;
        using (var scope = _scopeFactory.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            claims = await storage.ClaimScheduledAsync(
                GetBatchSize(),
                GetLockTimeout(),
                _timeProvider.GetUtcNow(),
                cancellationToken);
        }

        await Parallel.ForEachAsync(
            claims,
            new ParallelOptions
            {
                CancellationToken = cancellationToken,
                MaxDegreeOfParallelism = GetWorkerCount()
            },
            ProcessClaimAsync);

        return claims.Count;
    }

    private async ValueTask ProcessClaimAsync(MuleReplayClaim claim, CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();
        var action = MuleReplayModel.CreateAction(claim, now);

        try
        {
            using var scope = _scopeFactory.CreateScope();
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            var outcome = await storage.MaterializeAsync(claim, action, now, cancellationToken);
            if (outcome == MuleReplayMaterializationOutcome.LeaseLost)
                return;

            await _commitNotifier.NotifySavedAsync(action.Id, action.Lane, cancellationToken);
            _logger.LogInformation(
                "Mule replay {MuleReplayRequestId} materialized action {MuleReplayActionId} from {MuleActionId} with {MuleReplayMaterializationOutcome}.",
                claim.Replay.RequestId,
                action.Id,
                claim.Replay.SourceActionId,
                outcome);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            var nextAttemptOnUtc = now.Add(GetRetryDelay());
            try
            {
                using var scope = _scopeFactory.CreateScope();
                var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
                await storage.ReleaseAsync(claim, ex.ToString(), nextAttemptOnUtc, cancellationToken);
                _schedulerSignal.SignalReplayAt(nextAttemptOnUtc);
            }
            catch (Exception releaseException)
            {
                _logger.LogError(
                    releaseException,
                    "Mule replay {MuleReplayRequestId} lease could not be released.",
                    claim.Replay.RequestId);
            }

            _logger.LogError(
                ex,
                "Mule replay {MuleReplayRequestId} failed to materialize action {MuleReplayActionId}.",
                claim.Replay.RequestId,
                action.Id);
        }
    }

    private async Task<DateTimeOffset?> GetNextScheduledOnUtcAsync(CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
        return await storage.GetNextScheduledOnUtcAsync(
            GetLockTimeout(),
            _timeProvider.GetUtcNow(),
            cancellationToken);
    }

    private async Task WaitForSignalOrDueAsync(DateTimeOffset? dueOnUtc, CancellationToken cancellationToken)
    {
        if (dueOnUtc == null)
        {
            await _schedulerSignal.WaitForReplayAsync(cancellationToken);
            return;
        }

        var delay = dueOnUtc.Value - _timeProvider.GetUtcNow();
        if (delay <= TimeSpan.Zero)
            return;

        using var signalCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var signalTask = _schedulerSignal.WaitForReplayAsync(signalCancellation.Token).AsTask();
        var delayTask = Task.Delay(delay, _timeProvider, cancellationToken);
        var completed = await Task.WhenAny(signalTask, delayTask);
        if (completed == signalTask)
            await signalTask;
        else
            await signalCancellation.CancelAsync();
    }

    private int GetBatchSize() => Math.Max(1, _settings.BatchSize);

    private int GetWorkerCount() => Math.Max(1, _settings.WorkerCount);

    private TimeSpan GetLockTimeout()
        => _settings.LockTimeout <= TimeSpan.Zero ? TimeSpan.FromMinutes(1) : _settings.LockTimeout;

    private TimeSpan GetRetryDelay()
        => _settings.RetryDelay <= TimeSpan.Zero ? TimeSpan.FromSeconds(10) : _settings.RetryDelay;

    private TimeSpan GetPollingInterval()
        => _settings.PollingInterval <= TimeSpan.Zero ? TimeSpan.FromMinutes(1) : _settings.PollingInterval;
}
