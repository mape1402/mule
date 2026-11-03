namespace Mule.Redis;

using StackExchange.Redis;

internal sealed partial class RedisMuleStorage
{
    public async Task<MuleReplayResult> ScheduleAsync(
        Ulid sourceActionId,
        MuleReplayRequest request,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        if (request == null)
            throw new ArgumentNullException(nameof(request));

        EnsureCanWrite();
        cancellationToken.ThrowIfCancellationRequested();
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var branchId = request.AllowParallelReplay ? request.RequestId : Ulid.Empty;
        var replayActionId = Ulid.NewUlid();
        var schedule = request.ScheduleOnUtc is { } requested && requested > now ? requested : now;
        var result = (RedisResult[])await database.ScriptEvaluateAsync(
            RedisMuleScripts.ScheduleReplay,
            RedisKeys(_keys.ReplayDue, _keys.ReplayByAction, _keys.ReplayBranches),
            RedisValues(
                _keys.Root,
                request.RequestId.ToString(),
                Ulid.NewUlid().ToString(),
                sourceActionId.ToString(),
                replayActionId.ToString(),
                RedisMuleActionCodec.Milliseconds(now),
                branchId.ToString(),
                request.RequestedBy ?? string.Empty,
                request.Reason ?? string.Empty,
                RedisMuleReplayCodec.SerializeMetadata(request.Metadata) ?? string.Empty,
                RedisMuleActionCodec.Milliseconds(schedule),
                request.ScheduleOnUtc.HasValue ? 1 : 0,
                _keys.ReplayBranchField(sourceActionId, branchId)));
        cancellationToken.ThrowIfCancellationRequested();

        var code = (long)result[0];
        var persistedId = Ulid.TryParse(((RedisValue)result[1]).ToString(), out var parsed) ? parsed : Ulid.Empty;
        if (code == 0)
            await _connection.AcknowledgeReplicationAsync(database, cancellationToken);
        var replay = persistedId == Ulid.Empty
            ? null
            : RedisMuleReplayCodec.ToInfo(await ReadReplayAsync(database, persistedId, cancellationToken));
        return new MuleReplayResult
        {
            Outcome = code switch
            {
                0 => MuleReplayOutcome.Scheduled,
                1 => MuleReplayOutcome.AlreadyScheduled,
                2 => MuleReplayOutcome.NotFound,
                3 => MuleReplayOutcome.NotEligible,
                4 => MuleReplayOutcome.RequestConflict,
                _ => MuleReplayOutcome.ReplayAlreadyExists
            },
            Replay = replay
        };
    }

    public async Task<MuleReplayInfo> FindAsync(Ulid requestId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        return RedisMuleReplayCodec.ToInfo(await ReadReplayAsync(database, requestId, cancellationToken));
    }

    public async Task<IReadOnlyCollection<MuleReplayClaim>> ClaimScheduledAsync(
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        EnsureCanWrite();
        cancellationToken.ThrowIfCancellationRequested();
        batchSize = Math.Max(1, batchSize);
        lockTimeout = NormalizeTimeout(lockTimeout);
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var values = new List<RedisValue>(4 + batchSize * 2)
        {
            RedisMuleActionCodec.Milliseconds(now),
            batchSize,
            Math.Max(1L, (long)lockTimeout.TotalMilliseconds),
            _keys.Root
        };
        for (var index = 0; index < batchSize; index++)
        {
            values.Add(Ulid.NewUlid().ToString());
            values.Add(Ulid.NewUlid().ToString());
        }

        var result = await database.ScriptEvaluateAsync(
            RedisMuleScripts.ClaimReplays,
            RedisKeys(_keys.ReplayDue),
            values.ToArray());
        cancellationToken.ThrowIfCancellationRequested();
        var ids = ((RedisResult[])result)
            .Select(x => Ulid.Parse(((RedisValue)x).ToString()))
            .ToArray();
        if (ids.Length == 0)
            return Array.Empty<MuleReplayClaim>();

        await _connection.AcknowledgeReplicationAsync(database, cancellationToken);
        var claims = new List<MuleReplayClaim>(ids.Length);
        foreach (var id in ids)
        {
            var replay = await ReadReplayAsync(database, id, cancellationToken);
            if (replay == null)
                continue;
            var source = await ReadActionAsync(database, replay.SourceActionId, cancellationToken);
            if (source == null)
                continue;
            claims.Add(new MuleReplayClaim
            {
                Replay = replay,
                SourceAction = source,
                LockToken = replay.LockToken
            });
        }
        return claims;
    }

    public async Task<DateTimeOffset?> GetNextScheduledOnUtcAsync(
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var entries = await database.SortedSetRangeByRankWithScoresAsync(_keys.ReplayDue, 0, 0, Order.Ascending);
        cancellationToken.ThrowIfCancellationRequested();
        if (entries.Length == 0)
            return null;

        var due = DateTimeOffset.FromUnixTimeMilliseconds((long)entries[0].Score);
        return due <= now ? now : due;
    }

    public async Task<MuleReplayMaterializationOutcome> MaterializeAsync(
        MuleReplayClaim claim,
        DurableAction action,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        if (claim == null)
            throw new ArgumentNullException(nameof(claim));
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        EnsureCanWrite();
        cancellationToken.ThrowIfCancellationRequested();
        var lane = NormalizeLane(action.Lane);
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var result = (long)await database.ScriptEvaluateAsync(
            RedisMuleScripts.MaterializeReplay,
            RedisKeys(
                _keys.Replay(claim.Replay.RequestId),
                _keys.Action(action.Id),
                _keys.ActionDedupe,
                _keys.ActionsDue,
                _keys.Lanes,
                _keys.Pending,
                _keys.BacklogByLane,
                _keys.BacklogByActionKey,
                _keys.ReplayDue),
            RedisValues(
                claim.LockToken.ToString(),
                action.Id.ToString(),
                action.ConcurrencyToken.ToString(),
                action.Key.Value,
                lane,
                _keys.LaneDue(lane),
                action.Payload ?? string.Empty,
                action.PayloadType ?? string.Empty,
                action.Metadata ?? string.Empty,
                action.CorrelationId ?? string.Empty,
                action.DeduplicationKey ?? string.Empty,
                string.IsNullOrWhiteSpace(action.DeduplicationKey)
                    ? string.Empty
                    : _keys.DedupeField(action.Key, action.DeduplicationKey),
                RedisMuleActionCodec.Milliseconds(now),
                Ulid.NewUlid().ToString()));
        cancellationToken.ThrowIfCancellationRequested();
        if (result is 0 or 1)
            await _connection.AcknowledgeReplicationAsync(database, cancellationToken);
        return result switch
        {
            0 => MuleReplayMaterializationOutcome.Created,
            1 => MuleReplayMaterializationOutcome.Reconciled,
            _ => MuleReplayMaterializationOutcome.LeaseLost
        };
    }

    public async Task ReleaseAsync(
        MuleReplayClaim claim,
        string error,
        DateTimeOffset nextAttemptOnUtc,
        CancellationToken cancellationToken = default)
    {
        if (claim == null)
            throw new ArgumentNullException(nameof(claim));

        EnsureCanWrite();
        cancellationToken.ThrowIfCancellationRequested();
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var changed = (long)await database.ScriptEvaluateAsync(
            RedisMuleScripts.ReleaseReplay,
            RedisKeys(_keys.Replay(claim.Replay.RequestId), _keys.ReplayDue),
            RedisValues(
                claim.LockToken.ToString(),
                error ?? string.Empty,
                RedisMuleActionCodec.Milliseconds(nextAttemptOnUtc),
                Ulid.NewUlid().ToString()));
        if (changed > 0)
            await _connection.AcknowledgeReplicationAsync(database, cancellationToken);
    }

    private async Task<MuleReplay> ReadReplayAsync(
        IDatabase database,
        Ulid requestId,
        CancellationToken cancellationToken)
    {
        var replay = RedisMuleReplayCodec.FromHash(await database.HashGetAllAsync(_keys.Replay(requestId)));
        cancellationToken.ThrowIfCancellationRequested();
        return replay;
    }
}
