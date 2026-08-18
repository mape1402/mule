namespace Mule.FastLane.Redis;

internal sealed class FastLaneRedisStorage : IMuleStorage
{
    private readonly IMuleDurableStorage _durableStorage;
    private readonly RedisFastLaneBuffer _buffer;
    private readonly List<DurableAction> _pendingAdds = new();

    public FastLaneRedisStorage(IMuleDurableStorage durableStorage, RedisFastLaneBuffer buffer)
    {
        _durableStorage = durableStorage ?? throw new ArgumentNullException(nameof(durableStorage));
        _buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
    }

    public Task AddAsync(DurableAction action, CancellationToken cancellationToken = default)
    {
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        _pendingAdds.Add(action);
        return Task.CompletedTask;
    }

    public Task AddRangeAsync(IReadOnlyCollection<DurableAction> actions, CancellationToken cancellationToken = default)
    {
        if (actions == null)
            throw new ArgumentNullException(nameof(actions));

        foreach (var action in actions)
        {
            if (action == null)
                throw new ArgumentException("Action collection cannot contain null values.", nameof(actions));

            _pendingAdds.Add(action);
        }

        return Task.CompletedTask;
    }

    public async Task<Guid?> FindByDeduplicationKeyAsync(
        ActionKey key,
        string deduplicationKey,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(deduplicationKey))
            return null;

        var buffered = await _buffer.FindByDeduplicationKeyAsync(key, deduplicationKey);
        if (buffered != null)
            return buffered;

        return await _durableStorage.FindByDeduplicationKeyAsync(key, deduplicationKey, cancellationToken);
    }

    public async Task<IReadOnlyCollection<DurableAction>> ClaimPendingAsync(
        string lane,
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var buffered = await _buffer.ClaimPendingAsync(lane, batchSize, lockTimeout, now);
        if (buffered.Count >= batchSize)
            return buffered;

        var durable = await _durableStorage.ClaimPendingAsync(
            lane,
            batchSize - buffered.Count,
            lockTimeout,
            now,
            cancellationToken);

        return buffered.Concat(durable).ToArray();
    }

    public async Task<DateTimeOffset?> GetNextPendingOnUtcAsync(
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var buffered = await _buffer.GetNextPendingOnUtcAsync();
        var durable = await _durableStorage.GetNextPendingOnUtcAsync(lockTimeout, now, cancellationToken);

        if (buffered == null)
            return durable;

        if (durable == null)
            return buffered;

        return buffered <= durable ? buffered : durable;
    }

    public async Task<DurableAction> LockAsync(
        Guid id,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var buffered = await _buffer.LockAsync(id, lockTimeout, now);
        if (buffered != null)
            return buffered;

        return await _durableStorage.LockAsync(id, lockTimeout, now, cancellationToken);
    }

    public async Task MarkCompletedAsync(Guid id, DateTimeOffset completedOnUtc, CancellationToken cancellationToken = default)
    {
        if (await _buffer.MarkCompletedAsync(id, completedOnUtc))
            return;

        await _durableStorage.MarkCompletedAsync(id, completedOnUtc, cancellationToken);
    }

    public async Task MarkFailedAsync(
        Guid id,
        string error,
        DateTimeOffset now,
        DateTimeOffset? nextAttemptOnUtc,
        CancellationToken cancellationToken = default)
    {
        if (await _buffer.MarkFailedAsync(id, error, now, nextAttemptOnUtc))
            return;

        await _durableStorage.MarkFailedAsync(id, error, now, nextAttemptOnUtc, cancellationToken);
    }

    public Task<int> CleanCompletedAsync(DateTimeOffset olderThanUtc, int batchSize, CancellationToken cancellationToken = default)
        => _durableStorage.CleanCompletedAsync(olderThanUtc, batchSize, cancellationToken);

    public Task<DateTimeOffset?> GetOldestCompletedOnUtcAsync(CancellationToken cancellationToken = default)
        => _durableStorage.GetOldestCompletedOnUtcAsync(cancellationToken);

    public async Task SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        var accepted = new List<DurableAction>(_pendingAdds.Count);
        foreach (var action in _pendingAdds)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!string.IsNullOrWhiteSpace(action.DeduplicationKey))
            {
                var durableId = await _durableStorage.FindByDeduplicationKeyAsync(
                    action.Key,
                    action.DeduplicationKey,
                    cancellationToken);

                if (durableId != null)
                    continue;
            }

            accepted.Add(action);
        }

        await _buffer.TryAddRangeAsync(accepted);
        _pendingAdds.Clear();
        await _durableStorage.SaveChangesAsync(cancellationToken);
    }
}
