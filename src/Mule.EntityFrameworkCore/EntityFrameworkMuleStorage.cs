namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;

internal sealed class EntityFrameworkMuleStorage : IMuleStorage
{
    private readonly MuleDbContext _dbContext;

    public EntityFrameworkMuleStorage(MuleDbContext dbContext)
    {
        _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
    }

    public async Task AddAsync(DurableAction action, CancellationToken cancellationToken = default)
    {
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        if (!string.IsNullOrWhiteSpace(action.DeduplicationKey))
        {
            var exists = await _dbContext.Actions.AnyAsync(
                x => x.Key == action.Key && x.DeduplicationKey == action.DeduplicationKey,
                cancellationToken);

            if (exists)
                return;
        }

        await _dbContext.Actions.AddAsync(action, cancellationToken);
    }

    public async Task<IReadOnlyCollection<DurableAction>> LockPendingAsync(
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var lockExpiration = now.Subtract(lockTimeout);

        var candidates = await _dbContext.Actions
            .Where(x =>
                x.Status == DurableActionStatus.Pending && (x.NextAttemptOnUtc == null || x.NextAttemptOnUtc <= now) ||
                x.Status == DurableActionStatus.Locked && x.LockedOnUtc <= lockExpiration)
            .AsNoTracking()
            .ToListAsync(cancellationToken);

        return candidates
            .OrderBy(x => x.CreatedOnUtc)
            .Take(batchSize)
            .ToArray();
    }

    public async Task<DurableAction> LockAsync(
        Guid id,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var lockExpiration = now.Subtract(lockTimeout);
        var action = await _dbContext.Actions.FindAsync(new object[] { id }, cancellationToken);

        if (action == null)
            return null;

        var canLock =
            action.Status == DurableActionStatus.Pending && (action.NextAttemptOnUtc == null || action.NextAttemptOnUtc <= now) ||
            action.Status == DurableActionStatus.Locked && action.LockedOnUtc <= lockExpiration;

        if (!canLock)
            return null;

        action.Status = DurableActionStatus.Locked;
        action.LockedOnUtc = now;

        return action;
    }

    public async Task MarkCompletedAsync(Guid id, DateTimeOffset completedOnUtc, CancellationToken cancellationToken = default)
    {
        var action = await FindAsync(id, cancellationToken);
        action.Status = DurableActionStatus.Completed;
        action.CompletedOnUtc = completedOnUtc;
        action.LockedOnUtc = null;
        action.NextAttemptOnUtc = null;
        action.LastError = null;
    }

    public async Task MarkFailedAsync(
        Guid id,
        string error,
        DateTimeOffset now,
        DateTimeOffset? nextAttemptOnUtc,
        CancellationToken cancellationToken = default)
    {
        var action = await FindAsync(id, cancellationToken);
        action.Attempts++;
        action.Status = nextAttemptOnUtc == null ? DurableActionStatus.Failed : DurableActionStatus.Pending;
        action.LastError = error;
        action.LockedOnUtc = null;
        action.NextAttemptOnUtc = nextAttemptOnUtc;
    }

    public async Task<int> CleanCompletedAsync(DateTimeOffset olderThanUtc, int batchSize, CancellationToken cancellationToken = default)
    {
        var candidates = await _dbContext.Actions
            .Where(x => x.Status == DurableActionStatus.Completed && x.CompletedOnUtc <= olderThanUtc)
            .ToListAsync(cancellationToken);

        var actions = candidates
            .OrderBy(x => x.CompletedOnUtc)
            .Take(batchSize)
            .ToArray();

        _dbContext.Actions.RemoveRange(actions);
        return actions.Length;
    }

    public Task SaveChangesAsync(CancellationToken cancellationToken = default)
        => _dbContext.SaveChangesAsync(cancellationToken);

    private async Task<DurableAction> FindAsync(Guid id, CancellationToken cancellationToken)
        => await _dbContext.Actions.FindAsync(new object[] { id }, cancellationToken)
            ?? throw new InvalidOperationException($"Mule action '{id}' was not found.");
}
