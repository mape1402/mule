namespace Mule.Replay;

using Microsoft.Extensions.Logging;

internal sealed class MuleReplayService : IMuleReplayService
{
    private readonly IMuleReplayStorage _storage;
    private readonly IMuleReplayCommitNotifier _commitNotifier;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<MuleReplayService> _logger;

    public MuleReplayService(
        IMuleReplayStorage storage,
        IMuleReplayCommitNotifier commitNotifier,
        TimeProvider timeProvider,
        ILogger<MuleReplayService> logger)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _commitNotifier = commitNotifier ?? throw new ArgumentNullException(nameof(commitNotifier));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async ValueTask<MuleReplayResult> ReplayAsync(
        Ulid actionId,
        MuleReplayRequest request,
        CancellationToken cancellationToken = default)
    {
        if (actionId == Ulid.Empty)
            throw new ArgumentException("A source action ID is required.", nameof(actionId));
        if (request == null)
            throw new ArgumentNullException(nameof(request));
        if (request.RequestId == Ulid.Empty)
            throw new ArgumentException("A replay request ID is required.", nameof(request));

        var now = _timeProvider.GetUtcNow();
        var normalizedRequest = MuleReplayModel.NormalizeRequest(request, now);
        _logger.LogInformation(
            "Mule replay {MuleReplayRequestId} was requested for action {MuleActionId} by {MuleReplayRequestedBy}.",
            request.RequestId,
            actionId,
            request.RequestedBy);

        try
        {
            var result = await _storage.ScheduleAsync(actionId, normalizedRequest, now, cancellationToken);
            LogResult(result, actionId, request);

            if (result.Outcome == MuleReplayOutcome.Scheduled)
                await _commitNotifier.NotifyScheduledAsync(result.Replay.ScheduleOnUtc);

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Mule replay {MuleReplayRequestId} could not be scheduled for action {MuleActionId}.",
                request.RequestId,
                actionId);
            throw;
        }
    }

    public ValueTask<MuleReplayInfo> GetReplayAsync(
        Ulid requestId,
        CancellationToken cancellationToken = default)
    {
        if (requestId == Ulid.Empty)
            throw new ArgumentException("A replay request ID is required.", nameof(requestId));

        return new ValueTask<MuleReplayInfo>(_storage.FindAsync(requestId, cancellationToken));
    }

    private void LogResult(MuleReplayResult result, Ulid actionId, MuleReplayRequest request)
    {
        var replayActionId = result.Replay?.ReplayActionId ?? Ulid.Empty;
        switch (result.Outcome)
        {
            case MuleReplayOutcome.Scheduled:
                _logger.LogInformation(
                    "Mule replay {MuleReplayRequestId} scheduled action {MuleReplayActionId} from {MuleActionId} with key {MuleActionKey}, lane {MuleLane}, correlation {MuleCorrelationId}, requested by {MuleReplayRequestedBy}.",
                    request.RequestId,
                    replayActionId,
                    actionId,
                    result.Replay.ActionKey,
                    result.Replay.Lane,
                    result.Replay.CorrelationId,
                    result.Replay.RequestedBy);
                break;
            case MuleReplayOutcome.AlreadyScheduled:
                _logger.LogInformation(
                    "Mule replay {MuleReplayRequestId} returned existing action {MuleReplayActionId}.",
                    request.RequestId,
                    replayActionId);
                break;
            default:
                _logger.LogWarning(
                    "Mule replay {MuleReplayRequestId} for action {MuleActionId} was rejected with {MuleReplayOutcome}.",
                    request.RequestId,
                    actionId,
                    result.Outcome);
                break;
        }
    }
}
