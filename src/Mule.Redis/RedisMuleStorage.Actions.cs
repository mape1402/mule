namespace Mule.Redis;

using System.Transactions;
using Microsoft.Extensions.Options;
using Mule.Diagnostics;
using StackExchange.Redis;

internal sealed partial class RedisMuleStorage :
    IMuleDurableStorage,
    IMuleBatchTerminalStorage,
    IMuleReplayStorage,
    IMuleCancellationStorage,
    IMuleTerminalCancellationStorage
{
    private readonly RedisMuleConnection _connection;
    private readonly RedisMuleKeyspace _keys;
    private readonly MuleRuntimeMetrics _metrics;
    private readonly TimeSpan _configuredLockTimeout;
    private readonly List<DurableAction> _pendingAdds = new();

    public RedisMuleStorage(
        RedisMuleConnection connection,
        MuleRuntimeMetrics metrics,
        IOptions<MuleSettings> settings)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _keys = connection.Keyspace;
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        var configured = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
        _configuredLockTimeout = NormalizeTimeout(configured.LockTimeout);
    }

    public Task AddAsync(DurableAction action, CancellationToken cancellationToken = default)
    {
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        EnsureCanWrite();
        cancellationToken.ThrowIfCancellationRequested();
        _pendingAdds.Add(action);
        return Task.CompletedTask;
    }

    public Task AddRangeAsync(IReadOnlyCollection<DurableAction> actions, CancellationToken cancellationToken = default)
    {
        if (actions == null)
            throw new ArgumentNullException(nameof(actions));

        EnsureCanWrite();
        cancellationToken.ThrowIfCancellationRequested();
        foreach (var action in actions)
        {
            if (action == null)
                throw new ArgumentException("Action collection cannot contain null values.", nameof(actions));

            _pendingAdds.Add(action);
        }

        return Task.CompletedTask;
    }

    public async Task<Ulid?> FindByDeduplicationKeyAsync(
        ActionKey key,
        string deduplicationKey,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(deduplicationKey))
            return null;

        cancellationToken.ThrowIfCancellationRequested();
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var value = await database.HashGetAsync(_keys.ActionDedupe, _keys.DedupeField(key, deduplicationKey));
        cancellationToken.ThrowIfCancellationRequested();
        return Ulid.TryParse(value.ToString(), out var id) ? id : null;
    }

    public async Task<IReadOnlyCollection<DurableAction>> ClaimPendingAsync(
        string lane,
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lane = NormalizeLane(lane);
        batchSize = Math.Max(1, batchSize);
        lockTimeout = NormalizeTimeout(lockTimeout);
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var values = new List<RedisValue>(4 + batchSize)
        {
            RedisMuleActionCodec.Milliseconds(now),
            batchSize,
            Math.Max(1L, (long)lockTimeout.TotalMilliseconds),
            _keys.Root
        };
        values.AddRange(Enumerable.Range(0, batchSize).Select(_ => (RedisValue)Ulid.NewUlid().ToString()));

        var result = await database.ScriptEvaluateAsync(
            RedisMuleScripts.ClaimPending,
            RedisKeys(_keys.LaneDue(lane), _keys.ActionsDue, _keys.Pending, _keys.Locked, _keys.Latencies),
            values.ToArray());
        cancellationToken.ThrowIfCancellationRequested();

        var ids = ((RedisResult[])result)
            .Select(x => Ulid.Parse(((RedisValue)x).ToString()))
            .ToArray();
        return await ReadActionsAsync(database, ids, cancellationToken);
    }

    public async Task<DateTimeOffset?> GetNextPendingOnUtcAsync(
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var entries = await database.SortedSetRangeByRankWithScoresAsync(_keys.ActionsDue, 0, 0, Order.Ascending);
        cancellationToken.ThrowIfCancellationRequested();
        if (entries.Length == 0)
            return null;

        var due = DateTimeOffset.FromUnixTimeMilliseconds((long)entries[0].Score);
        return due <= now ? now : due;
    }

    public async Task<DurableAction> LockAsync(
        Ulid id,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lockTimeout = NormalizeTimeout(lockTimeout);
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var changed = (long)await database.ScriptEvaluateAsync(
            RedisMuleScripts.LockAction,
            RedisKeys(_keys.Action(id), _keys.ActionsDue, _keys.Lanes, _keys.Pending, _keys.Locked, _keys.Latencies),
            RedisValues(
                RedisMuleActionCodec.Milliseconds(now),
                Math.Max(1L, (long)lockTimeout.TotalMilliseconds),
                Ulid.NewUlid().ToString()));
        cancellationToken.ThrowIfCancellationRequested();
        if (changed == 0)
            return null;

        return await ReadActionAsync(database, id, cancellationToken);
    }

    public Task MarkCompletedAsync(
        Ulid id,
        DateTimeOffset completedOnUtc,
        CancellationToken cancellationToken = default)
        => MarkCompletedRangeAsync([new MuleCompletedAction(id, completedOnUtc)], cancellationToken);

    public async Task MarkCompletedRangeAsync(
        IReadOnlyCollection<MuleCompletedAction> actions,
        CancellationToken cancellationToken = default)
    {
        if (actions == null)
            throw new ArgumentNullException(nameof(actions));
        if (actions.Count == 0)
            return;

        EnsureCanWrite();
        cancellationToken.ThrowIfCancellationRequested();
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        foreach (var batch in actions.Chunk(_connection.WriteBatchSize))
        {
            var values = new List<RedisValue>(2 + batch.Length * 3) { batch.Length };
            foreach (var action in batch)
            {
                values.Add(action.Id.ToString());
                values.Add(RedisMuleActionCodec.Milliseconds(action.CompletedOnUtc));
                values.Add(Ulid.NewUlid().ToString());
            }
            values.Add(_keys.Root);

            var changed = (long)await database.ScriptEvaluateAsync(
                RedisMuleScripts.MarkCompleted,
                RedisKeys(
                    _keys.ActionsDue,
                    _keys.Pending,
                    _keys.Locked,
                    _keys.Completed,
                    _keys.BacklogByLane,
                    _keys.BacklogByActionKey,
                    _keys.Latencies),
                values.ToArray());
            if (changed > 0)
                await _connection.AcknowledgeReplicationAsync(database, cancellationToken);
        }
    }

    public async Task MarkFailedAsync(
        Ulid id,
        string error,
        DateTimeOffset now,
        DateTimeOffset? nextAttemptOnUtc,
        CancellationToken cancellationToken = default)
    {
        EnsureCanWrite();
        cancellationToken.ThrowIfCancellationRequested();
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var changed = (long)await database.ScriptEvaluateAsync(
            RedisMuleScripts.MarkFailed,
            RedisKeys(
                _keys.Action(id),
                _keys.ActionsDue,
                _keys.BacklogByLane,
                _keys.Pending,
                _keys.Locked,
                _keys.Failed,
                _keys.BacklogByActionKey,
                _keys.RetriesByActionKey,
                _keys.FailuresByActionKey,
                _keys.Latencies),
            RedisValues(
                RedisMuleActionCodec.Milliseconds(now),
                error ?? string.Empty,
                RedisMuleActionCodec.Milliseconds(nextAttemptOnUtc),
                Ulid.NewUlid().ToString()));
        cancellationToken.ThrowIfCancellationRequested();
        if (changed > 0)
            await _connection.AcknowledgeReplicationAsync(database, cancellationToken);
        else if (!await database.KeyExistsAsync(_keys.Action(id)))
            throw new InvalidOperationException($"Mule action '{id}' was not found.");
    }

    public async Task<int> CleanCompletedAsync(
        DateTimeOffset olderThanUtc,
        int batchSize,
        CancellationToken cancellationToken = default)
    {
        EnsureCanWrite();
        cancellationToken.ThrowIfCancellationRequested();
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var deleted = (long)await database.ScriptEvaluateAsync(
            RedisMuleScripts.CleanCompleted,
            RedisKeys(_keys.Completed, _keys.ActionDedupe, _keys.RetriesByActionKey, _keys.Latencies),
            RedisValues(
                RedisMuleActionCodec.Milliseconds(olderThanUtc),
                Math.Max(1, batchSize),
                _keys.Root));
        if (deleted > 0)
            await _connection.AcknowledgeReplicationAsync(database, cancellationToken);
        return checked((int)deleted);
    }

    public async Task<DateTimeOffset?> GetOldestCompletedOnUtcAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var entries = await database.SortedSetRangeByRankWithScoresAsync(_keys.Completed, 0, 0, Order.Ascending);
        cancellationToken.ThrowIfCancellationRequested();
        return entries.Length == 0
            ? null
            : DateTimeOffset.FromUnixTimeMilliseconds((long)entries[0].Score);
    }

    public async Task SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        if (_pendingAdds.Count == 0)
            return;

        EnsureCanWrite();
        cancellationToken.ThrowIfCancellationRequested();
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var pending = _pendingAdds.ToArray();
        foreach (var batch in pending.Chunk(_connection.WriteBatchSize))
        {
            var values = new List<RedisValue>(2 + batch.Length * RedisMuleActionCodec.ArgumentCount)
            {
                _keys.Root,
                batch.Length
            };
            foreach (var action in batch)
                values.AddRange(RedisMuleActionCodec.ToArguments(action, _keys, _configuredLockTimeout));

            var result = (RedisResult[])await database.ScriptEvaluateAsync(
                RedisMuleScripts.AddActions,
                RedisKeys(
                    _keys.ActionDedupe,
                    _keys.ActionsDue,
                    _keys.Lanes,
                    _keys.Pending,
                    _keys.Locked,
                    _keys.Completed,
                    _keys.Failed,
                    _keys.Cancelled,
                    _keys.BacklogByLane,
                    _keys.BacklogByActionKey),
                values.ToArray());
            var added = (long)result[0];
            var duplicates = (long)result[1];
            for (var index = 0L; index < duplicates; index++)
                _metrics.RecordDuplicateIgnored();
            if (added > 0)
                await _connection.AcknowledgeReplicationAsync(database, cancellationToken);
        }

        _pendingAdds.RemoveRange(0, pending.Length);
    }

    private async Task<IReadOnlyCollection<DurableAction>> ReadActionsAsync(
        IDatabase database,
        IReadOnlyCollection<Ulid> ids,
        CancellationToken cancellationToken)
    {
        if (ids.Count == 0)
            return Array.Empty<DurableAction>();

        var reads = ids.Select(id => database.HashGetAllAsync(_keys.Action(id))).ToArray();
        var entries = await Task.WhenAll(reads);
        cancellationToken.ThrowIfCancellationRequested();
        return entries
            .Select(RedisMuleActionCodec.FromHash)
            .Where(x => x != null)
            .ToArray();
    }

    private async Task<DurableAction> ReadActionAsync(
        IDatabase database,
        Ulid id,
        CancellationToken cancellationToken)
    {
        var action = RedisMuleActionCodec.FromHash(await database.HashGetAllAsync(_keys.Action(id)));
        cancellationToken.ThrowIfCancellationRequested();
        return action;
    }

    private void EnsureCanWrite()
    {
        if (Transaction.Current != null && _connection.AmbientTransactions == RedisAmbientTransactionMode.Reject)
        {
            throw new InvalidOperationException(
                "Redis Mule storage cannot participate atomically in an ambient transaction. " +
                "Complete the transaction before enqueueing, or explicitly configure AmbientTransactions = RedisAmbientTransactionMode.Immediate.");
        }
    }

    private static TimeSpan NormalizeTimeout(TimeSpan value)
        => value <= TimeSpan.Zero ? TimeSpan.FromMinutes(5) : value;

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;

    private static RedisKey[] RedisKeys(params string[] values)
        => values.Select(x => (RedisKey)x).ToArray();

    private static RedisValue[] RedisValues(params RedisValue[] values)
        => values;
}
