namespace Mule.FastLane.InMemory;

internal sealed class FastLaneInMemoryStorage : IMuleStorage, IMuleExecutionStorage, IMuleCancellationStorage
{
    private readonly IMuleDurableStorage _durableStorage;
    private readonly InMemoryFastLaneBuffer _buffer;
    private readonly List<DurableAction> _pendingAdds = new();

    public FastLaneInMemoryStorage(IMuleDurableStorage durableStorage, InMemoryFastLaneBuffer buffer)
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

    public async Task<Ulid?> FindByDeduplicationKeyAsync(
        ActionKey key,
        string deduplicationKey,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(deduplicationKey))
            return null;

        if (_buffer.TryFindByDeduplicationKey(key, deduplicationKey, out var id))
            return id;

        return await _durableStorage.FindByDeduplicationKeyAsync(key, deduplicationKey, cancellationToken);
    }

    public async Task<IReadOnlyCollection<DurableAction>> ClaimPendingAsync(
        string lane,
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var buffered = _buffer.ClaimPending(lane, batchSize, lockTimeout, now);
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
        var buffered = _buffer.GetNextPendingOnUtc(lockTimeout, now);
        var durable = await _durableStorage.GetNextPendingOnUtcAsync(lockTimeout, now, cancellationToken);

        if (buffered == null)
            return durable;

        if (durable == null)
            return buffered;

        return buffered.Value <= durable.Value ? buffered.Value : durable.Value;
    }

    public async Task<DurableAction> LockAsync(
        Ulid id,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var buffered = _buffer.Lock(id, lockTimeout, now);
        if (buffered != null)
            return buffered;

        return await _durableStorage.LockAsync(id, lockTimeout, now, cancellationToken);
    }

    public async Task<IReadOnlyCollection<DurableAction>> ReadPendingCandidatesAsync(
        string lane,
        int offset,
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var take = checked(Math.Max(0, offset) + Math.Max(1, batchSize));
        var buffered = _buffer.ReadPendingCandidates(lane, 0, take, lockTimeout, now);
        var durable = await ExecutionStorage.ReadPendingCandidatesAsync(
            lane, 0, take, lockTimeout, now, cancellationToken);
        return buffered
            .Concat(durable)
            .GroupBy(x => x.Id)
            .Select(x => x.First())
            .OrderBy(x => x.NextAttemptOnUtc ?? x.CreatedOnUtc)
            .ThenBy(x => x.CreatedOnUtc)
            .ThenBy(x => x.Id)
            .Skip(Math.Max(0, offset))
            .Take(Math.Max(1, batchSize))
            .ToArray();
    }

    public async Task<DurableAction> FindActionAsync(Ulid id, CancellationToken cancellationToken = default)
        => _buffer.FindAction(id) ?? await ExecutionStorage.FindActionAsync(id, cancellationToken);

    public async Task<MuleActionTransitionOutcome> CompleteClaimAsync(
        Ulid id,
        Ulid expectedConcurrencyToken,
        DateTimeOffset completedOnUtc,
        CancellationToken cancellationToken = default)
    {
        var buffered = _buffer.CompleteClaim(id, expectedConcurrencyToken, completedOnUtc);
        return buffered == MuleActionTransitionOutcome.NotFound
            ? await ExecutionStorage.CompleteClaimAsync(id, expectedConcurrencyToken, completedOnUtc, cancellationToken)
            : buffered;
    }

    public async Task<MuleActionTransitionOutcome> FailClaimAsync(
        Ulid id,
        Ulid expectedConcurrencyToken,
        string error,
        DateTimeOffset now,
        DateTimeOffset? nextAttemptOnUtc,
        CancellationToken cancellationToken = default)
    {
        var buffered = _buffer.FailClaim(id, expectedConcurrencyToken, error, now, nextAttemptOnUtc);
        return buffered == MuleActionTransitionOutcome.NotFound
            ? await ExecutionStorage.FailClaimAsync(id, expectedConcurrencyToken, error, now, nextAttemptOnUtc, cancellationToken)
            : buffered;
    }

    public async Task<MuleActionTransitionOutcome> DeferClaimAsync(
        Ulid id,
        Ulid expectedConcurrencyToken,
        DateTimeOffset deferredOnUtc,
        DateTimeOffset notBeforeUtc,
        DeferralReason reason,
        CancellationToken cancellationToken = default)
    {
        var buffered = _buffer.DeferClaim(id, expectedConcurrencyToken, notBeforeUtc, reason);
        return buffered == MuleActionTransitionOutcome.NotFound
            ? await ExecutionStorage.DeferClaimAsync(
                id, expectedConcurrencyToken, deferredOnUtc, notBeforeUtc, reason, cancellationToken)
            : buffered;
    }

    public async Task MarkCompletedAsync(Ulid id, DateTimeOffset completedOnUtc, CancellationToken cancellationToken = default)
    {
        if (_buffer.MarkCompleted(id, completedOnUtc))
            return;

        await _durableStorage.MarkCompletedAsync(id, completedOnUtc, cancellationToken);
    }

    public async Task MarkFailedAsync(
        Ulid id,
        string error,
        DateTimeOffset now,
        DateTimeOffset? nextAttemptOnUtc,
        CancellationToken cancellationToken = default)
    {
        if (_buffer.MarkFailed(id, error, now, nextAttemptOnUtc))
            return;

        await _durableStorage.MarkFailedAsync(id, error, now, nextAttemptOnUtc, cancellationToken);
    }

    public async Task<MuleCancellationResult> CancelAsync(
        Ulid actionId,
        DateTimeOffset cancelledOnUtc,
        CancellationToken cancellationToken = default)
    {
        if (_buffer.TryCancel(actionId, cancelledOnUtc, out var result))
            return result;

        if (_durableStorage is not IMuleCancellationStorage cancellationStorage)
            return new MuleCancellationResult(actionId, MuleCancellationOutcome.NotSupported);

        return await cancellationStorage.CancelAsync(actionId, cancelledOnUtc, cancellationToken);
    }

    public Task<int> CleanCompletedAsync(DateTimeOffset olderThanUtc, int batchSize, CancellationToken cancellationToken = default)
        => _durableStorage.CleanCompletedAsync(olderThanUtc, batchSize, cancellationToken);

    public Task<DateTimeOffset?> GetOldestCompletedOnUtcAsync(CancellationToken cancellationToken = default)
        => _durableStorage.GetOldestCompletedOnUtcAsync(cancellationToken);

    public async Task SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        foreach (var action in _pendingAdds)
        {
            if (!string.IsNullOrWhiteSpace(action.DeduplicationKey))
            {
                var durableId = await _durableStorage.FindByDeduplicationKeyAsync(
                    action.Key,
                    action.DeduplicationKey,
                    cancellationToken);

                if (durableId != null)
                    continue;
            }

            _buffer.TryAdd(action);
        }

        _pendingAdds.Clear();
        await _durableStorage.SaveChangesAsync(cancellationToken);
    }

    private IMuleExecutionStorage ExecutionStorage
        => _durableStorage as IMuleExecutionStorage
            ?? throw new NotSupportedException(
                $"Durable storage '{_durableStorage.GetType().FullName}' does not support keyed execution or deferral.");
}
