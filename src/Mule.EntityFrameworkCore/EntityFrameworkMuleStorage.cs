namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

internal class EntityFrameworkMuleStorage<TDbContext> : IMuleStorage, IDisposable, IAsyncDisposable
    where TDbContext : DbContext
{
    private readonly TDbContext _dbContext;

    public EntityFrameworkMuleStorage(IMuleDbContextFactory<TDbContext> dbContextFactory)
    {
        if (dbContextFactory == null)
            throw new ArgumentNullException(nameof(dbContextFactory));

        _dbContext = dbContextFactory.CreateDbContext();
    }

    public async Task AddAsync(DurableAction action, CancellationToken cancellationToken = default)
    {
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        if (!string.IsNullOrWhiteSpace(action.DeduplicationKey))
        {
            var exists = await Actions.AnyAsync(
                x => x.Key == action.Key && x.DeduplicationKey == action.DeduplicationKey,
                cancellationToken);

            if (exists)
                return;
        }

        await Actions.AddAsync(action, cancellationToken);
    }

    public async Task<IReadOnlyCollection<DurableAction>> LockPendingAsync(
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var lockExpiration = now.Subtract(lockTimeout);

        var candidates = await Actions
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
        var locked = _dbContext.Database.IsRelational()
            ? await LockRelationalAsync(id, lockExpiration, now, cancellationToken)
            : await LockTrackedAsync(id, lockExpiration, now, cancellationToken);

        if (locked == 0)
            return null;

        return await Actions.FindAsync(new object[] { id }, cancellationToken);
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
        var candidates = await Actions
            .Where(x => x.Status == DurableActionStatus.Completed && x.CompletedOnUtc <= olderThanUtc)
            .ToListAsync(cancellationToken);

        var actions = candidates
            .OrderBy(x => x.CompletedOnUtc)
            .Take(batchSize)
            .ToArray();

        Actions.RemoveRange(actions);
        return actions.Length;
    }

    public Task SaveChangesAsync(CancellationToken cancellationToken = default)
        => _dbContext.SaveChangesAsync(cancellationToken);

    protected DbSet<DurableAction> Actions => _dbContext.Set<DurableAction>();

    private async Task<int> LockRelationalAsync(
        Guid id,
        DateTimeOffset lockExpiration,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        var names = GetActionStoreNames();
        var sql = $$"""
UPDATE {{names.Table}}
SET {{names.Status}} = {0},
    {{names.LockedOnUtc}} = {1}
WHERE {{names.Id}} = {2}
  AND (
      ({{names.Status}} = {3} AND ({{names.NextAttemptOnUtc}} IS NULL OR {{names.NextAttemptOnUtc}} <= {4}))
      OR ({{names.Status}} = {5} AND {{names.LockedOnUtc}} <= {6})
  )
""";

        return await _dbContext.Database.ExecuteSqlRawAsync(
            sql,
            [
                (int)DurableActionStatus.Locked,
                now,
                id,
                (int)DurableActionStatus.Pending,
                now,
                (int)DurableActionStatus.Locked,
                lockExpiration
            ],
            cancellationToken);
    }

    private async Task<int> LockTrackedAsync(
        Guid id,
        DateTimeOffset lockExpiration,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        var action = await Actions.FindAsync(new object[] { id }, cancellationToken);

        if (action == null)
            return 0;

        var canLock =
            action.Status == DurableActionStatus.Pending && (action.NextAttemptOnUtc == null || action.NextAttemptOnUtc <= now) ||
            action.Status == DurableActionStatus.Locked && action.LockedOnUtc <= lockExpiration;

        if (!canLock)
            return 0;

        action.Status = DurableActionStatus.Locked;
        action.LockedOnUtc = now;
        await _dbContext.SaveChangesAsync(cancellationToken);
        return 1;
    }

    private ActionStoreNames GetActionStoreNames()
    {
        var entity = _dbContext.Model.FindEntityType(typeof(DurableAction))
            ?? throw new InvalidOperationException("Mule action entity is not part of the DbContext model.");
        var tableName = entity.GetTableName()
            ?? throw new InvalidOperationException("Mule action entity is not mapped to a table.");
        var schema = entity.GetSchema();
        var table = StoreObjectIdentifier.Table(tableName, schema);
        var helper = _dbContext.GetService<ISqlGenerationHelper>();

        string Column(string propertyName)
        {
            var property = entity.FindProperty(propertyName)
                ?? throw new InvalidOperationException($"Mule action property '{propertyName}' is not part of the DbContext model.");
            return helper.DelimitIdentifier(property.GetColumnName(table), schema: null);
        }

        return new ActionStoreNames(
            helper.DelimitIdentifier(tableName, schema),
            Column(nameof(DurableAction.Id)),
            Column(nameof(DurableAction.Status)),
            Column(nameof(DurableAction.LockedOnUtc)),
            Column(nameof(DurableAction.NextAttemptOnUtc)));
    }

    private async Task<DurableAction> FindAsync(Guid id, CancellationToken cancellationToken)
        => await Actions.FindAsync(new object[] { id }, cancellationToken)
            ?? throw new InvalidOperationException($"Mule action '{id}' was not found.");

    public void Dispose()
        => _dbContext.Dispose();

    public ValueTask DisposeAsync()
        => _dbContext.DisposeAsync();

    private sealed record ActionStoreNames(
        string Table,
        string Id,
        string Status,
        string LockedOnUtc,
        string NextAttemptOnUtc);
}

internal sealed class EntityFrameworkMuleStorage : EntityFrameworkMuleStorage<MuleDbContext>
{
    public EntityFrameworkMuleStorage(IMuleDbContextFactory<MuleDbContext> dbContextFactory)
        : base(dbContextFactory)
    {
    }
}
