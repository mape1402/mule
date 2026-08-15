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

    public async Task<IReadOnlyCollection<DurableAction>> ClaimPendingAsync(
        string lane,
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var lockExpiration = now.Subtract(lockTimeout);

        if (IsSqlServer())
            return await ClaimSqlServerAsync(lane, batchSize, lockExpiration, now, cancellationToken);

        return await ClaimTrackedAsync(lane, batchSize, lockExpiration, now, cancellationToken);
    }

    public async Task<DateTimeOffset?> GetNextPendingOnUtcAsync(
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var lockExpiration = now.Subtract(lockTimeout);
        var actions = await Actions
            .AsNoTracking()
            .Where(x => x.Status == DurableActionStatus.Pending || x.Status == DurableActionStatus.Locked)
            .Select(x => new
            {
                x.Status,
                x.LockedOnUtc,
                x.NextAttemptOnUtc
            })
            .ToListAsync(cancellationToken);

        return actions
            .Select(x =>
            {
                if (x.Status == DurableActionStatus.Pending)
                    return x.NextAttemptOnUtc == null || x.NextAttemptOnUtc <= now
                        ? now
                        : x.NextAttemptOnUtc;

                if (x.LockedOnUtc <= lockExpiration)
                    return now;

                return x.LockedOnUtc?.Add(lockTimeout);
            })
            .Where(x => x != null)
            .OrderBy(x => x)
            .FirstOrDefault();
    }

    private async Task<IReadOnlyCollection<DurableAction>> ClaimTrackedAsync(
        string lane,
        int batchSize,
        DateTimeOffset lockExpiration,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        var candidates = await Actions
            .Where(x =>
                x.Lane == NormalizeLane(lane) &&
                (x.Status == DurableActionStatus.Pending && (x.NextAttemptOnUtc == null || x.NextAttemptOnUtc <= now) ||
                x.Status == DurableActionStatus.Locked && x.LockedOnUtc <= lockExpiration))
            .OrderBy(x => x.CreatedOnUtc)
            .Take(batchSize)
            .ToListAsync(cancellationToken);

        foreach (var action in candidates)
        {
            action.Status = DurableActionStatus.Locked;
            action.LockedOnUtc = now;
            action.StartedOnUtc ??= now;
        }

        await _dbContext.SaveChangesAsync(cancellationToken);
        return candidates.ToArray();
    }

    public async Task MarkCompletedAsync(Guid id, DateTimeOffset completedOnUtc, CancellationToken cancellationToken = default)
    {
        var action = await FindAsync(id, cancellationToken);
        action.Status = DurableActionStatus.Completed;
        action.CompletedOnUtc = completedOnUtc;
        action.TerminalOnUtc = completedOnUtc;
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
        action.TerminalOnUtc = nextAttemptOnUtc == null ? now : null;
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

    public async Task<DateTimeOffset?> GetOldestCompletedOnUtcAsync(CancellationToken cancellationToken = default)
        => await Actions
            .AsNoTracking()
            .Where(x => x.Status == DurableActionStatus.Completed && x.CompletedOnUtc != null)
            .OrderBy(x => x.CompletedOnUtc)
            .Select(x => x.CompletedOnUtc)
            .FirstOrDefaultAsync(cancellationToken);

    public async Task SaveChangesAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _dbContext.SaveChangesAsync(cancellationToken);
        }
        catch (DbUpdateException ex) when (IsUniqueConstraintViolation(ex))
        {
            foreach (var entry in _dbContext.ChangeTracker.Entries<DurableAction>().Where(x => x.State == EntityState.Added))
                entry.State = EntityState.Detached;
        }
    }

    protected DbSet<DurableAction> Actions => _dbContext.Set<DurableAction>();

    private async Task<IReadOnlyCollection<DurableAction>> ClaimSqlServerAsync(
        string lane,
        int batchSize,
        DateTimeOffset lockExpiration,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        var names = GetActionStoreNames();
        var sql = $$"""
WITH MuleClaim AS (
    SELECT TOP ({0}) *
    FROM {{names.Table}} WITH (UPDLOCK, READPAST, ROWLOCK)
    WHERE {{names.Lane}} = {3}
      AND (
          ({{names.Status}} = {4} AND ({{names.NextAttemptOnUtc}} IS NULL OR {{names.NextAttemptOnUtc}} <= {5}))
          OR ({{names.Status}} = {6} AND {{names.LockedOnUtc}} <= {7})
      )
    ORDER BY COALESCE({{names.NextAttemptOnUtc}}, {{names.CreatedOnUtc}}), {{names.CreatedOnUtc}}
)
UPDATE MuleClaim
SET {{names.Status}} = {1},
    {{names.LockedOnUtc}} = {2},
    {{names.StartedOnUtc}} = COALESCE({{names.StartedOnUtc}}, {2})
OUTPUT INSERTED.*
""";

        return await Actions
            .FromSqlRaw(
                sql,
                batchSize,
                (int)DurableActionStatus.Locked,
                now,
                NormalizeLane(lane),
                (int)DurableActionStatus.Pending,
                now,
                (int)DurableActionStatus.Locked,
                lockExpiration)
            .AsNoTracking()
            .ToListAsync(cancellationToken);
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
    {{names.LockedOnUtc}} = {1},
    {{names.StartedOnUtc}} = COALESCE({{names.StartedOnUtc}}, {1})
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
        action.StartedOnUtc ??= now;
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
            Column(nameof(DurableAction.Lane)),
            Column(nameof(DurableAction.Status)),
            Column(nameof(DurableAction.LockedOnUtc)),
            Column(nameof(DurableAction.StartedOnUtc)),
            Column(nameof(DurableAction.CreatedOnUtc)),
            Column(nameof(DurableAction.NextAttemptOnUtc)));
    }

    private async Task<DurableAction> FindAsync(Guid id, CancellationToken cancellationToken)
        => await Actions.FindAsync(new object[] { id }, cancellationToken)
            ?? throw new InvalidOperationException($"Mule action '{id}' was not found.");

    public void Dispose()
        => _dbContext.Dispose();

    public ValueTask DisposeAsync()
        => _dbContext.DisposeAsync();

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;

    private static bool IsUniqueConstraintViolation(DbUpdateException exception)
        => exception.InnerException?.Message.Contains("unique", StringComparison.OrdinalIgnoreCase) == true ||
           exception.InnerException?.Message.Contains("duplicate", StringComparison.OrdinalIgnoreCase) == true;

    private bool IsSqlServer()
        => _dbContext.Database.ProviderName?.Contains("SqlServer", StringComparison.OrdinalIgnoreCase) == true;

    private sealed record ActionStoreNames(
        string Table,
        string Id,
        string Lane,
        string Status,
        string LockedOnUtc,
        string StartedOnUtc,
        string CreatedOnUtc,
        string NextAttemptOnUtc);
}

internal sealed class EntityFrameworkMuleStorage : EntityFrameworkMuleStorage<MuleDbContext>
{
    public EntityFrameworkMuleStorage(IMuleDbContextFactory<MuleDbContext> dbContextFactory)
        : base(dbContextFactory)
    {
    }
}
