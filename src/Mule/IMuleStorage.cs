namespace Mule;

public interface IMuleStorage
{
    Task AddAsync(DurableAction action, CancellationToken cancellationToken = default);

    Task AddRangeAsync(IReadOnlyCollection<DurableAction> actions, CancellationToken cancellationToken = default);

    Task<Ulid?> FindByDeduplicationKeyAsync(
        ActionKey key,
        string deduplicationKey,
        CancellationToken cancellationToken = default);

    Task<IReadOnlyCollection<DurableAction>> ClaimPendingAsync(
        string lane,
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default);

    Task<DateTimeOffset?> GetNextPendingOnUtcAsync(
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default);

    Task<DurableAction> LockAsync(
        Ulid id,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default);

    Task MarkCompletedAsync(Ulid id, DateTimeOffset completedOnUtc, CancellationToken cancellationToken = default);

    Task MarkFailedAsync(
        Ulid id,
        string error,
        DateTimeOffset now,
        DateTimeOffset? nextAttemptOnUtc,
        CancellationToken cancellationToken = default);

    Task<int> CleanCompletedAsync(DateTimeOffset olderThanUtc, int batchSize, CancellationToken cancellationToken = default);

    Task<DateTimeOffset?> GetOldestCompletedOnUtcAsync(CancellationToken cancellationToken = default);

    Task SaveChangesAsync(CancellationToken cancellationToken = default);
}
