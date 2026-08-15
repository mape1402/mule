namespace Mule.InMemory;

using System.Transactions;

internal sealed class InMemoryMuleStorage : IMuleStorage
{
    private readonly InMemoryMuleStore _store;
    private readonly List<DurableAction> _pendingAdds = new();

    public InMemoryMuleStorage(InMemoryMuleStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
    }

    public Task AddAsync(DurableAction action, CancellationToken cancellationToken = default)
    {
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        _pendingAdds.Add(action);
        return Task.CompletedTask;
    }

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

    public Task<DurableAction> LockAsync(Guid id, TimeSpan lockTimeout, DateTimeOffset now, CancellationToken cancellationToken = default)
        => Task.FromResult(_store.Lock(id, lockTimeout, now));

    public Task MarkCompletedAsync(Guid id, DateTimeOffset completedOnUtc, CancellationToken cancellationToken = default)
    {
        _store.MarkCompleted(id, completedOnUtc);
        return Task.CompletedTask;
    }

    public Task MarkFailedAsync(
        Guid id,
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
            _store.Add(action);
    }
}
