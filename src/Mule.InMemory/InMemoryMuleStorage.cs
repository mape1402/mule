namespace Mule.InMemory;

using System.Transactions;
using Mule.Diagnostics;

internal sealed class InMemoryMuleStorage : IMuleDurableStorage, IMuleReplayStorage
{
    private readonly InMemoryMuleStore _store;
    private readonly MuleRuntimeMetrics _metrics;
    private readonly List<DurableAction> _pendingAdds = new();

    public InMemoryMuleStorage(InMemoryMuleStore store, MuleRuntimeMetrics metrics)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
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

    public Task<Ulid?> FindByDeduplicationKeyAsync(
        ActionKey key,
        string deduplicationKey,
        CancellationToken cancellationToken = default)
        => Task.FromResult(_store.FindByDeduplicationKey(key, deduplicationKey));

    public Task<IReadOnlyCollection<DurableAction>> ClaimPendingAsync(
        string lane,
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
        => Task.FromResult(_store.ClaimPending(lane, batchSize, lockTimeout, now));

    public Task<DateTimeOffset?> GetNextPendingOnUtcAsync(
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
        => Task.FromResult(_store.GetNextPendingOnUtc(lockTimeout, now));

    public Task<DurableAction> LockAsync(Ulid id, TimeSpan lockTimeout, DateTimeOffset now, CancellationToken cancellationToken = default)
        => Task.FromResult(_store.Lock(id, lockTimeout, now));

    public Task MarkCompletedAsync(Ulid id, DateTimeOffset completedOnUtc, CancellationToken cancellationToken = default)
    {
        _store.MarkCompleted(id, completedOnUtc);
        return Task.CompletedTask;
    }

    public Task MarkFailedAsync(
        Ulid id,
        string error,
        DateTimeOffset now,
        DateTimeOffset? nextAttemptOnUtc,
        CancellationToken cancellationToken = default)
    {
        _store.MarkFailed(id, error, nextAttemptOnUtc);
        return Task.CompletedTask;
    }

    public Task<int> CleanCompletedAsync(DateTimeOffset olderThanUtc, int batchSize, CancellationToken cancellationToken = default)
        => Task.FromResult(_store.CleanCompleted(olderThanUtc, batchSize));

    public Task<DateTimeOffset?> GetOldestCompletedOnUtcAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(_store.GetOldestCompletedOnUtc());

    public Task<MuleReplayResult> ScheduleAsync(
        Ulid sourceActionId,
        MuleReplayRequest request,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
        => Task.FromResult(_store.ScheduleReplay(sourceActionId, request, now));

    public Task<MuleReplayInfo> FindAsync(Ulid requestId, CancellationToken cancellationToken = default)
        => Task.FromResult(_store.FindReplay(requestId));

    public Task<IReadOnlyCollection<MuleReplayClaim>> ClaimScheduledAsync(
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
        => Task.FromResult(_store.ClaimScheduledReplays(batchSize, lockTimeout, now));

    public Task<DateTimeOffset?> GetNextScheduledOnUtcAsync(
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
        => Task.FromResult(_store.GetNextScheduledReplayOnUtc(lockTimeout, now));

    public Task<MuleReplayMaterializationOutcome> MaterializeAsync(
        MuleReplayClaim claim,
        DurableAction action,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
        => Task.FromResult(_store.MaterializeReplay(claim, action, now));

    public Task ReleaseAsync(
        MuleReplayClaim claim,
        string error,
        DateTimeOffset nextAttemptOnUtc,
        CancellationToken cancellationToken = default)
    {
        _store.ReleaseReplay(claim, error, nextAttemptOnUtc);
        return Task.CompletedTask;
    }

    public Task SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        if (_pendingAdds.Count == 0)
            return Task.CompletedTask;

        var actions = _pendingAdds.Select(InMemoryMuleStore.Clone).ToArray();
        _pendingAdds.Clear();

        var transaction = Transaction.Current;
        if (transaction == null)
        {
            Commit(actions);
            return Task.CompletedTask;
        }

        transaction.TransactionCompleted += (_, args) =>
        {
            if (args.Transaction.TransactionInformation.Status == TransactionStatus.Committed)
                Commit(actions);
        };

        return Task.CompletedTask;
    }

    private void Commit(IEnumerable<DurableAction> actions)
    {
        foreach (var action in actions)
        {
            if (!_store.Add(action))
                _metrics.RecordDuplicateIgnored();
        }
    }
}
