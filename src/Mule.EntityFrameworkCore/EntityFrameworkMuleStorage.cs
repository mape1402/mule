namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Mule.Diagnostics;

internal class EntityFrameworkMuleStorage<TDbContext> : IMuleDurableStorage, IMuleBatchTerminalStorage, IDisposable, IAsyncDisposable
    where TDbContext : DbContext
{
    private readonly TDbContext _dbContext;
    private readonly MuleRuntimeMetrics _metrics;

    public EntityFrameworkMuleStorage(IMuleDbContextFactory<TDbContext> dbContextFactory, MuleRuntimeMetrics metrics)
    {
        if (dbContextFactory == null)
            throw new ArgumentNullException(nameof(dbContextFactory));

        _dbContext = dbContextFactory.CreateDbContext();
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
    }

    public async Task AddAsync(DurableAction action, CancellationToken cancellationToken = default)
    {
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        await AddRangeAsync([action], cancellationToken);
    }

    public async Task AddRangeAsync(IReadOnlyCollection<DurableAction> actions, CancellationToken cancellationToken = default)
    {
        if (actions == null)
            throw new ArgumentNullException(nameof(actions));

        if (actions.Count == 0)
            return;

        foreach (var action in actions)
        {
            if (action == null)
                throw new ArgumentException("Action collection cannot contain null values.", nameof(actions));
        }

        var deduplicatedActions = actions
            .Where(x => !string.IsNullOrWhiteSpace(x.DeduplicationKey))
            .ToArray();

        if (deduplicatedActions.Length > 0)
        {
            var keys = deduplicatedActions
                .Select(x => x.Key)
                .Distinct()
                .ToArray();
            var deduplicationKeys = deduplicatedActions
                .Select(x => x.DeduplicationKey)
                .Distinct(StringComparer.Ordinal)
                .ToArray();
            var existing = await Actions
                .AsNoTracking()
                .Where(x => keys.Contains(x.Key) && deduplicationKeys.Contains(x.DeduplicationKey))
                .Select(x => new { x.Key, x.DeduplicationKey })
                .ToListAsync(cancellationToken);
            var existingKeys = existing
                .Select(x => (x.Key, x.DeduplicationKey))
                .ToHashSet();

            var newKeys = new HashSet<(ActionKey Key, string DeduplicationKey)>();
            foreach (var action in deduplicatedActions)
            {
                var key = (action.Key, action.DeduplicationKey);
                if (existingKeys.Contains(key) || !newKeys.Add(key))
                {
                    _metrics.RecordDuplicateIgnored();
                    continue;
                }

                await Actions.AddAsync(action, cancellationToken);
            }

            var nonDeduplicatedActions = actions
                .Where(x => string.IsNullOrWhiteSpace(x.DeduplicationKey))
                .ToArray();
            await Actions.AddRangeAsync(nonDeduplicatedActions, cancellationToken);
            return;
        }

        await Actions.AddRangeAsync(actions, cancellationToken);
    }

    public async Task<Guid?> FindByDeduplicationKeyAsync(
        ActionKey key,
        string deduplicationKey,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(deduplicationKey))
            return null;

        return await Actions
            .AsNoTracking()
            .Where(x => x.Key == key && x.DeduplicationKey == deduplicationKey)
            .Select(x => (Guid?)x.Id)
            .FirstOrDefaultAsync(cancellationToken);
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
        var normalizedLane = NormalizeLane(lane);
        var candidates = await Actions
            .Where(x =>
                x.Lane == normalizedLane &&
                (x.Status == DurableActionStatus.Pending || x.Status == DurableActionStatus.Locked))
            .ToListAsync(cancellationToken);

        candidates = candidates
            .Where(x =>
                x.Status == DurableActionStatus.Pending && (x.NextAttemptOnUtc == null || x.NextAttemptOnUtc <= now) ||
                x.Status == DurableActionStatus.Locked && x.LockedOnUtc <= lockExpiration)
            .OrderBy(x => x.NextAttemptOnUtc ?? x.CreatedOnUtc)
            .ThenBy(x => x.CreatedOnUtc)
            .Take(batchSize)
            .ToList();

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
        if (IsSqlServer())
        {
            await MarkCompletedSqlServerAsync(id, completedOnUtc, cancellationToken);
            return;
        }

        var action = await FindAsync(id, cancellationToken);
        action.Status = DurableActionStatus.Completed;
        action.CompletedOnUtc = completedOnUtc;
        action.TerminalOnUtc = completedOnUtc;
        action.LockedOnUtc = null;
        action.NextAttemptOnUtc = null;
        action.LastError = null;
    }

    public async Task MarkCompletedRangeAsync(
        IReadOnlyCollection<MuleCompletedAction> actions,
        CancellationToken cancellationToken = default)
    {
        if (actions == null)
            throw new ArgumentNullException(nameof(actions));

        if (actions.Count == 0)
            return;

        if (IsSqlServer())
        {
            await MarkCompletedRangeSqlServerAsync(actions, cancellationToken);
            return;
        }

        foreach (var completed in actions)
            await MarkCompletedAsync(completed.Id, completed.CompletedOnUtc, cancellationToken);
    }

    public async Task MarkFailedAsync(
        Guid id,
        string error,
        DateTimeOffset now,
        DateTimeOffset? nextAttemptOnUtc,
        CancellationToken cancellationToken = default)
    {
        if (IsSqlServer())
        {
            await MarkFailedSqlServerAsync(id, error, now, nextAttemptOnUtc, cancellationToken);
            return;
        }

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
        if (IsSqlServer())
            return await CleanCompletedSqlServerAsync(olderThanUtc, batchSize, cancellationToken);

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

            _metrics.RecordDuplicateIgnored();
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

    private async Task MarkCompletedSqlServerAsync(
        Guid id,
        DateTimeOffset completedOnUtc,
        CancellationToken cancellationToken)
    {
        var names = GetActionStoreNames();
        var sql = $$"""
UPDATE {{names.Table}}
SET {{names.Status}} = {0},
    {{names.CompletedOnUtc}} = {1},
    {{names.TerminalOnUtc}} = {1},
    {{names.LockedOnUtc}} = NULL,
    {{names.NextAttemptOnUtc}} = NULL,
    {{names.LastError}} = NULL
WHERE {{names.Id}} = {2}
""";

        await _dbContext.Database.ExecuteSqlRawAsync(
            sql,
            [
                (int)DurableActionStatus.Completed,
                completedOnUtc,
                id
            ],
            cancellationToken);
    }

    private async Task MarkCompletedRangeSqlServerAsync(
        IReadOnlyCollection<MuleCompletedAction> actions,
        CancellationToken cancellationToken)
    {
        var names = GetActionStoreNames();
        var values = new List<string>(actions.Count);
        var parameters = new object[actions.Count * 2 + 1];
        var index = 0;
        parameters[index++] = (int)DurableActionStatus.Completed;

        foreach (var action in actions)
        {
            values.Add($"({{{index}}}, {{{index + 1}}})");
            parameters[index++] = action.Id;
            parameters[index++] = action.CompletedOnUtc;
        }

        var sql = $$"""
UPDATE target
SET target.{{names.Status}} = {0},
    target.{{names.CompletedOnUtc}} = source.CompletedOnUtc,
    target.{{names.TerminalOnUtc}} = source.CompletedOnUtc,
    target.{{names.LockedOnUtc}} = NULL,
    target.{{names.NextAttemptOnUtc}} = NULL,
    target.{{names.LastError}} = NULL
FROM {{names.Table}} AS target
INNER JOIN (VALUES {{string.Join(", ", values)}}) AS source(Id, CompletedOnUtc)
    ON target.{{names.Id}} = source.Id
""";

        await _dbContext.Database.ExecuteSqlRawAsync(sql, parameters, cancellationToken);
    }

    private async Task MarkFailedSqlServerAsync(
        Guid id,
        string error,
        DateTimeOffset now,
        DateTimeOffset? nextAttemptOnUtc,
        CancellationToken cancellationToken)
    {
        var names = GetActionStoreNames();
        var sql = $$"""
UPDATE {{names.Table}}
SET {{names.Attempts}} = {{names.Attempts}} + 1,
    {{names.Status}} = {0},
    {{names.LastError}} = {1},
    {{names.LockedOnUtc}} = NULL,
    {{names.NextAttemptOnUtc}} = {2},
    {{names.TerminalOnUtc}} = {3}
WHERE {{names.Id}} = {4}
""";

        await _dbContext.Database.ExecuteSqlRawAsync(
            sql,
            [
                (int)(nextAttemptOnUtc == null ? DurableActionStatus.Failed : DurableActionStatus.Pending),
                error,
                nextAttemptOnUtc,
                nextAttemptOnUtc == null ? now : null,
                id
            ],
            cancellationToken);
    }

    private async Task<int> CleanCompletedSqlServerAsync(
        DateTimeOffset olderThanUtc,
        int batchSize,
        CancellationToken cancellationToken)
    {
        var names = GetActionStoreNames();
        var sql = $$"""
DELETE TOP ({0})
FROM {{names.Table}}
WHERE {{names.Status}} = {1}
  AND {{names.CompletedOnUtc}} <= {2}
""";

        return await _dbContext.Database.ExecuteSqlRawAsync(
            sql,
            [
                Math.Max(1, batchSize),
                (int)DurableActionStatus.Completed,
                olderThanUtc
            ],
            cancellationToken);
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
            Column(nameof(DurableAction.NextAttemptOnUtc)),
            Column(nameof(DurableAction.CompletedOnUtc)),
            Column(nameof(DurableAction.TerminalOnUtc)),
            Column(nameof(DurableAction.LastError)),
            Column(nameof(DurableAction.Attempts)));
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
        string NextAttemptOnUtc,
        string CompletedOnUtc,
        string TerminalOnUtc,
        string LastError,
        string Attempts);
}

internal sealed class EntityFrameworkMuleStorage : EntityFrameworkMuleStorage<MuleDbContext>
{
    public EntityFrameworkMuleStorage(IMuleDbContextFactory<MuleDbContext> dbContextFactory, MuleRuntimeMetrics metrics)
        : base(dbContextFactory, metrics)
    {
    }
}
