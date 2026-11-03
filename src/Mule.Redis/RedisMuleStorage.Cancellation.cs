namespace Mule.Redis;

using StackExchange.Redis;

internal sealed partial class RedisMuleStorage
{
    public async Task<MuleCancellationResult> CancelAsync(
        Ulid actionId,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        EnsureCanWrite();
        cancellationToken.ThrowIfCancellationRequested();
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var actionResult = await CancelActionAsync(database, actionId, now, false, cancellationToken);
        if (actionResult.Outcome != MuleCancellationOutcome.NotFound)
            return actionResult;

        var result = (RedisResult[])await database.ScriptEvaluateAsync(
            RedisMuleScripts.CancelReplay,
            RedisKeys(_keys.ReplayByAction, _keys.ReplayDue),
            RedisValues(
                actionId.ToString(),
                _keys.Root,
                RedisMuleActionCodec.Milliseconds(now),
                Ulid.NewUlid().ToString()));
        cancellationToken.ThrowIfCancellationRequested();
        var mapped = MapCancellation(actionId, result);
        if (mapped.Outcome == MuleCancellationOutcome.Cancelled)
            await _connection.AcknowledgeReplicationAsync(database, cancellationToken);
        return mapped;
    }

    public async Task<bool> MarkCancelledAsync(
        Ulid actionId,
        DateTimeOffset cancelledOnUtc,
        CancellationToken cancellationToken = default)
    {
        EnsureCanWrite();
        cancellationToken.ThrowIfCancellationRequested();
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var result = await CancelActionAsync(database, actionId, cancelledOnUtc, true, cancellationToken);
        return result.Outcome is MuleCancellationOutcome.Cancelled or MuleCancellationOutcome.AlreadyCancelled;
    }

    private async Task<MuleCancellationResult> CancelActionAsync(
        IDatabase database,
        Ulid actionId,
        DateTimeOffset now,
        bool allowLocked,
        CancellationToken cancellationToken)
    {
        var result = (RedisResult[])await database.ScriptEvaluateAsync(
            RedisMuleScripts.CancelAction,
            RedisKeys(
                _keys.Action(actionId),
                _keys.ActionsDue,
                _keys.Pending,
                _keys.Locked,
                _keys.Cancelled,
                _keys.BacklogByLane,
                _keys.BacklogByActionKey,
                _keys.Latencies),
            RedisValues(
                RedisMuleActionCodec.Milliseconds(now),
                allowLocked ? 1 : 0,
                Ulid.NewUlid().ToString()));
        cancellationToken.ThrowIfCancellationRequested();
        var mapped = MapCancellation(actionId, result);
        if (mapped.Outcome == MuleCancellationOutcome.Cancelled)
            await _connection.AcknowledgeReplicationAsync(database, cancellationToken);
        return mapped;
    }

    private static MuleCancellationResult MapCancellation(Ulid actionId, RedisResult[] result)
    {
        var code = (long)result[0];
        var cancelledOnUtc = long.TryParse(((RedisValue)result[1]).ToString(), out var milliseconds)
            ? DateTimeOffset.FromUnixTimeMilliseconds(milliseconds)
            : (DateTimeOffset?)null;
        var outcome = code switch
        {
            1 => MuleCancellationOutcome.Cancelled,
            2 => MuleCancellationOutcome.AlreadyCancelled,
            3 => MuleCancellationOutcome.TooLate,
            _ => MuleCancellationOutcome.NotFound
        };
        return new MuleCancellationResult(actionId, outcome, cancelledOnUtc);
    }
}
