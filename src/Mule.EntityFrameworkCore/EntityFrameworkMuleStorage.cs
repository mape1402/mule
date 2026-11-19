namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Mule.Diagnostics;

internal partial class EntityFrameworkMuleStorage<TDbContext> :
    IMuleDurableStorage,
    IMuleExecutionStorage,
    IMuleStateProjectionStorage,
    IMuleBatchTerminalStorage,
    IMuleReplayStorage,
    IMuleCancellationStorage,
    IMuleTerminalCancellationStorage,
    IDisposable,
    IAsyncDisposable
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

        var ids = actions
            .Select(x => x.Id)
            .Distinct()
            .ToArray();
        var existingIds = await Actions
            .AsNoTracking()
            .Where(x => ids.Contains(x.Id))
            .Select(x => x.Id)
            .ToListAsync(cancellationToken);
        var newActionIds = new HashSet<Ulid>();
        var newActions = actions
            .Where(x => !existingIds.Contains(x.Id) && newActionIds.Add(x.Id))
            .ToArray();

        if (newActions.Length == 0)
            return;

        var deduplicatedActions = newActions
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

            var nonDeduplicatedActions = newActions
                .Where(x => string.IsNullOrWhiteSpace(x.DeduplicationKey))
                .ToArray();
            await Actions.AddRangeAsync(nonDeduplicatedActions, cancellationToken);
            return;
        }

        await Actions.AddRangeAsync(newActions, cancellationToken);
    }

    public async Task<Ulid?> FindByDeduplicationKeyAsync(
        ActionKey key,
        string deduplicationKey,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(deduplicationKey))
            return null;

        return await Actions
            .AsNoTracking()
            .Where(x => x.Key == key && x.DeduplicationKey == deduplicationKey)
            .Select(x => (Ulid?)x.Id)
            .FirstOrDefaultAsync(cancellationToken);
    }

    public async Task<IReadOnlyCollection<DurableAction>> ClaimPendingAsync(
        string lane,
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var normalizedLane = NormalizeLane(lane);
        var lockExpiration = now.Subtract(lockTimeout);
        var candidateIds = await GetClaimCandidateIdsAsync(
            normalizedLane,
            Math.Max(1, batchSize),
            lockExpiration,
            now,
            cancellationToken);

        if (candidateIds.Length == 0)
            return Array.Empty<DurableAction>();

        var claimed = new List<DurableAction>(candidateIds.Length);
        foreach (var id in candidateIds)
        {
            var action = await LockAsync(id, lockTimeout, now, cancellationToken);
            if (action != null)
                claimed.Add(action);
        }

        return claimed;
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
            .Select(x => GetNextPendingOnUtc(x.Status, x.LockedOnUtc, x.NextAttemptOnUtc, lockExpiration, lockTimeout, now))
            .Where(x => x != null)
            .OrderBy(x => x)
            .FirstOrDefault();
    }

    public async Task<DurableAction> LockAsync(
        Ulid id,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var lockExpiration = now.Subtract(lockTimeout);
        var action = await Actions.FindAsync(new object[] { id }, cancellationToken);

        if (action == null)
            return null;

        if (!CanLock(action.Status, action.LockedOnUtc, action.NextAttemptOnUtc, lockExpiration, now))
            return null;

        action.Status = DurableActionStatus.Locked;
        action.LockedOnUtc = now;
        action.StartedOnUtc ??= now;
        action.PendingReason = DurableActionPendingReason.None;
        action.DeferralReasonCode = null;
        action.ConcurrencyToken = Ulid.NewUlid();

        try
        {
            await _dbContext.SaveChangesAsync(cancellationToken);
            return action;
        }
        catch (DbUpdateConcurrencyException)
        {
            DetachChangedEntries();
            return null;
        }
    }

    public async Task MarkCompletedAsync(Ulid id, DateTimeOffset completedOnUtc, CancellationToken cancellationToken = default)
    {
        var action = await FindAsync(id, cancellationToken);
        action.Status = DurableActionStatus.Completed;
        action.CompletedOnUtc = completedOnUtc;
        action.TerminalOnUtc = completedOnUtc;
        action.LockedOnUtc = null;
        action.NextAttemptOnUtc = null;
        action.PendingReason = DurableActionPendingReason.None;
        action.DeferralReasonCode = null;
        action.LastError = null;
        action.ConcurrencyToken = Ulid.NewUlid();
    }

    public async Task MarkCompletedRangeAsync(
        IReadOnlyCollection<MuleCompletedAction> actions,
        CancellationToken cancellationToken = default)
    {
        if (actions == null)
            throw new ArgumentNullException(nameof(actions));

        if (actions.Count == 0)
            return;

        foreach (var completed in actions)
            await MarkCompletedAsync(completed.Id, completed.CompletedOnUtc, cancellationToken);
    }

    public async Task MarkFailedAsync(
        Ulid id,
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
        action.PendingReason = nextAttemptOnUtc == null
            ? DurableActionPendingReason.None
            : DurableActionPendingReason.Retry;
        action.DeferralReasonCode = null;
        action.TerminalOnUtc = nextAttemptOnUtc == null ? now : null;
        action.ConcurrencyToken = Ulid.NewUlid();
    }

    public async Task<int> CleanCompletedAsync(DateTimeOffset olderThanUtc, int batchSize, CancellationToken cancellationToken = default)
    {
        var ids = await GetCompletedCandidateIdsAsync(
            olderThanUtc,
            Math.Max(1, batchSize),
            cancellationToken);

        if (ids.Length == 0)
            return 0;

        var actions = await Actions
            .Where(x => ids.Contains(x.Id))
            .ToArrayAsync(cancellationToken);
        Actions.RemoveRange(actions);
        return actions.Length;
    }

    public async Task<DateTimeOffset?> GetOldestCompletedOnUtcAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await Actions
                .AsNoTracking()
                .Where(x => x.Status == DurableActionStatus.Completed && x.CompletedOnUtc != null)
                .OrderBy(x => x.CompletedOnUtc)
                .Select(x => x.CompletedOnUtc)
                .FirstOrDefaultAsync(cancellationToken);
        }
        catch (Exception ex) when (ShouldFallbackToClientFiltering(ex))
        {
            var completed = await Actions
                .AsNoTracking()
                .Where(x => x.Status == DurableActionStatus.Completed && x.CompletedOnUtc != null)
                .Select(x => new { x.CompletedOnUtc })
                .ToListAsync(cancellationToken);

            return completed
                .OrderBy(x => x.CompletedOnUtc)
                .Select(x => x.CompletedOnUtc)
                .FirstOrDefault();
        }
    }

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

    public async Task<IReadOnlyCollection<DurableAction>> ReadPendingCandidatesAsync(
        string lane,
        int offset,
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var normalizedLane = NormalizeLane(lane);
        var lockExpiration = now.Subtract(lockTimeout);
        try
        {
            return await Actions
                .AsNoTracking()
                .Where(x =>
                    x.Lane == normalizedLane &&
                    ((x.Status == DurableActionStatus.Pending && (x.NextAttemptOnUtc == null || x.NextAttemptOnUtc <= now)) ||
                     (x.Status == DurableActionStatus.Locked && x.LockedOnUtc != null && x.LockedOnUtc <= lockExpiration)))
                .OrderBy(x => x.NextAttemptOnUtc ?? x.CreatedOnUtc)
                .ThenBy(x => x.CreatedOnUtc)
                .ThenBy(x => x.Id)
                .Skip(Math.Max(0, offset))
                .Take(Math.Max(1, batchSize))
                .ToArrayAsync(cancellationToken);
        }
        catch (Exception ex) when (ShouldFallbackToClientFiltering(ex))
        {
            var candidates = await Actions
                .AsNoTracking()
                .Where(x => x.Lane == normalizedLane &&
                    (x.Status == DurableActionStatus.Pending || x.Status == DurableActionStatus.Locked))
                .ToListAsync(cancellationToken);
            return candidates
                .Where(x => CanLock(x.Status, x.LockedOnUtc, x.NextAttemptOnUtc, lockExpiration, now))
                .OrderBy(x => x.NextAttemptOnUtc ?? x.CreatedOnUtc)
                .ThenBy(x => x.CreatedOnUtc)
                .ThenBy(x => x.Id)
                .Skip(Math.Max(0, offset))
                .Take(Math.Max(1, batchSize))
                .ToArray();
        }
    }

    public Task<DurableAction> FindActionAsync(Ulid id, CancellationToken cancellationToken = default)
        => Actions.AsNoTracking().FirstOrDefaultAsync(x => x.Id == id, cancellationToken);

    public Task<MuleActionTransitionOutcome> CompleteClaimAsync(
        Ulid id,
        Ulid expectedConcurrencyToken,
        DateTimeOffset completedOnUtc,
        CancellationToken cancellationToken = default)
        => ApplyClaimTransitionAsync(
            id,
            expectedConcurrencyToken,
            action =>
            {
                action.Status = DurableActionStatus.Completed;
                action.CompletedOnUtc = completedOnUtc;
                action.TerminalOnUtc = completedOnUtc;
                action.LockedOnUtc = null;
                action.NextAttemptOnUtc = null;
                action.PendingReason = DurableActionPendingReason.None;
                action.DeferralReasonCode = null;
                action.LastError = null;
            },
            cancellationToken);

    public Task<MuleActionTransitionOutcome> FailClaimAsync(
        Ulid id,
        Ulid expectedConcurrencyToken,
        string error,
        DateTimeOffset now,
        DateTimeOffset? nextAttemptOnUtc,
        CancellationToken cancellationToken = default)
        => ApplyClaimTransitionAsync(
            id,
            expectedConcurrencyToken,
            action =>
            {
                action.Attempts++;
                action.Status = nextAttemptOnUtc == null ? DurableActionStatus.Failed : DurableActionStatus.Pending;
                action.LastError = error;
                action.LockedOnUtc = null;
                action.NextAttemptOnUtc = nextAttemptOnUtc;
                action.PendingReason = nextAttemptOnUtc == null
                    ? DurableActionPendingReason.None
                    : DurableActionPendingReason.Retry;
                action.DeferralReasonCode = null;
                action.TerminalOnUtc = nextAttemptOnUtc == null ? now : null;
            },
            cancellationToken);

    public Task<MuleActionTransitionOutcome> DeferClaimAsync(
        Ulid id,
        Ulid expectedConcurrencyToken,
        DateTimeOffset deferredOnUtc,
        DateTimeOffset notBeforeUtc,
        DeferralReason reason,
        CancellationToken cancellationToken = default)
        => ApplyClaimTransitionAsync(
            id,
            expectedConcurrencyToken,
            action =>
            {
                action.Status = DurableActionStatus.Pending;
                action.LockedOnUtc = null;
                action.NextAttemptOnUtc = notBeforeUtc;
                action.PendingReason = DurableActionPendingReason.Deferred;
                action.DeferralReasonCode = reason.Value;
                action.TerminalOnUtc = null;
            },
            cancellationToken);

    public async Task<IReadOnlyCollection<MuleActionStateProjectionResult>> ProjectStateRangeAsync(
        IReadOnlyCollection<MuleActionStateProjection> projections,
        CancellationToken cancellationToken = default)
    {
        if (projections == null)
            throw new ArgumentNullException(nameof(projections));
        if (projections.Count == 0)
            return Array.Empty<MuleActionStateProjectionResult>();
        if (projections.Any(x => x == null))
            throw new ArgumentException("Projection collection cannot contain null values.", nameof(projections));

        var duplicateId = projections
            .GroupBy(x => x.Action.Id)
            .FirstOrDefault(x => x.Count() > 1);
        if (duplicateId != null)
            throw new ArgumentException($"Action '{duplicateId.Key}' occurs more than once in the projection batch.", nameof(projections));

        var ids = projections.Select(x => x.Action.Id).ToArray();
        var existing = await Actions
            .Where(x => ids.Contains(x.Id))
            .ToDictionaryAsync(x => x.Id, cancellationToken);
        var results = new List<MuleActionStateProjectionResult>(projections.Count);
        var changed = new HashSet<Ulid>();

        foreach (var projection in projections)
        {
            var state = projection.Action;
            if (!existing.TryGetValue(state.Id, out var current))
            {
                if (projection.ExpectedConcurrencyToken != null)
                {
                    results.Add(new(state.Id, MuleActionTransitionOutcome.NotFound));
                    continue;
                }

                await Actions.AddAsync(CloneState(state), cancellationToken);
                changed.Add(state.Id);
                results.Add(new(state.Id, MuleActionTransitionOutcome.Applied));
                continue;
            }

            if (current.ConcurrencyToken == state.ConcurrencyToken)
            {
                results.Add(new(state.Id, MuleActionTransitionOutcome.Applied));
                continue;
            }

            if (projection.ExpectedConcurrencyToken is { } expected)
            {
                if (current.ConcurrencyToken != expected)
                {
                    results.Add(new(state.Id, MuleActionTransitionOutcome.LeaseLost));
                    continue;
                }
            }
            else if (current.ConcurrencyToken != state.ConcurrencyToken)
            {
                results.Add(new(state.Id, MuleActionTransitionOutcome.LeaseLost));
                continue;
            }

            CopyState(state, current);
            changed.Add(state.Id);
            results.Add(new(state.Id, MuleActionTransitionOutcome.Applied));
        }

        if (changed.Count == 0)
            return results;

        try
        {
            await _dbContext.SaveChangesAsync(cancellationToken);
            return results;
        }
        catch (DbUpdateConcurrencyException)
        {
            DetachChangedEntries();
            return results
                .Select(x => changed.Contains(x.ActionId)
                    ? x with { Outcome = MuleActionTransitionOutcome.LeaseLost }
                    : x)
                .ToArray();
        }
    }

    protected DbSet<DurableAction> Actions => _dbContext.Set<DurableAction>();

    private static DurableAction CloneState(DurableAction source)
    {
        var clone = new DurableAction();
        CopyState(source, clone);
        return clone;
    }

    private static void CopyState(DurableAction source, DurableAction target)
    {
        target.Id = source.Id;
        target.ConcurrencyToken = source.ConcurrencyToken;
        target.Key = source.Key;
        target.Lane = source.Lane;
        target.Payload = source.Payload;
        target.PayloadType = source.PayloadType;
        target.Metadata = source.Metadata;
        target.CorrelationId = source.CorrelationId;
        target.DeduplicationKey = source.DeduplicationKey;
        target.Status = source.Status;
        target.Attempts = source.Attempts;
        target.LastError = source.LastError;
        target.CreatedOnUtc = source.CreatedOnUtc;
        target.LockedOnUtc = source.LockedOnUtc;
        target.StartedOnUtc = source.StartedOnUtc;
        target.NextAttemptOnUtc = source.NextAttemptOnUtc;
        target.PendingReason = source.PendingReason;
        target.DeferralReasonCode = source.DeferralReasonCode;
        target.CompletedOnUtc = source.CompletedOnUtc;
        target.CancelledOnUtc = source.CancelledOnUtc;
        target.TerminalOnUtc = source.TerminalOnUtc;
    }

    private async Task<Ulid[]> GetClaimCandidateIdsAsync(
        string normalizedLane,
        int batchSize,
        DateTimeOffset lockExpiration,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        try
        {
            return await Actions
                .AsNoTracking()
                .Where(x =>
                    x.Lane == normalizedLane &&
                    ((x.Status == DurableActionStatus.Pending && (x.NextAttemptOnUtc == null || x.NextAttemptOnUtc <= now)) ||
                     (x.Status == DurableActionStatus.Locked && x.LockedOnUtc != null && x.LockedOnUtc <= lockExpiration)))
                .OrderBy(x => x.NextAttemptOnUtc ?? x.CreatedOnUtc)
                .ThenBy(x => x.CreatedOnUtc)
                .Select(x => x.Id)
                .Take(batchSize)
                .ToArrayAsync(cancellationToken);
        }
        catch (Exception ex) when (ShouldFallbackToClientFiltering(ex))
        {
            var candidates = await Actions
                .AsNoTracking()
                .Where(x =>
                    x.Lane == normalizedLane &&
                    (x.Status == DurableActionStatus.Pending || x.Status == DurableActionStatus.Locked))
                .Select(x => new
                {
                    x.Id,
                    x.Status,
                    x.CreatedOnUtc,
                    x.LockedOnUtc,
                    x.NextAttemptOnUtc
                })
                .ToListAsync(cancellationToken);

            return candidates
                .Where(x => CanLock(x.Status, x.LockedOnUtc, x.NextAttemptOnUtc, lockExpiration, now))
                .OrderBy(x => x.NextAttemptOnUtc ?? x.CreatedOnUtc)
                .ThenBy(x => x.CreatedOnUtc)
                .Select(x => x.Id)
                .Take(batchSize)
                .ToArray();
        }
    }

    private async Task<Ulid[]> GetCompletedCandidateIdsAsync(
        DateTimeOffset olderThanUtc,
        int batchSize,
        CancellationToken cancellationToken)
    {
        try
        {
            return await Actions
                .AsNoTracking()
                .Where(x => x.Status == DurableActionStatus.Completed && x.CompletedOnUtc != null && x.CompletedOnUtc <= olderThanUtc)
                .OrderBy(x => x.CompletedOnUtc)
                .ThenBy(x => x.CreatedOnUtc)
                .Select(x => x.Id)
                .Take(batchSize)
                .ToArrayAsync(cancellationToken);
        }
        catch (Exception ex) when (ShouldFallbackToClientFiltering(ex))
        {
            var candidates = await Actions
                .AsNoTracking()
                .Where(x => x.Status == DurableActionStatus.Completed && x.CompletedOnUtc != null)
                .Select(x => new
                {
                    x.Id,
                    x.CreatedOnUtc,
                    x.CompletedOnUtc
                })
                .ToListAsync(cancellationToken);

            return candidates
                .Where(x => x.CompletedOnUtc <= olderThanUtc)
                .OrderBy(x => x.CompletedOnUtc)
                .ThenBy(x => x.CreatedOnUtc)
                .Select(x => x.Id)
                .Take(batchSize)
                .ToArray();
        }
    }

    private async Task<DurableAction> FindAsync(Ulid id, CancellationToken cancellationToken)
        => await Actions.FindAsync(new object[] { id }, cancellationToken)
            ?? throw new InvalidOperationException($"Mule action '{id}' was not found.");

    private async Task<MuleActionTransitionOutcome> ApplyClaimTransitionAsync(
        Ulid id,
        Ulid expectedConcurrencyToken,
        Action<DurableAction> transition,
        CancellationToken cancellationToken)
    {
        var action = await Actions.FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
        if (action == null)
            return MuleActionTransitionOutcome.NotFound;
        if (action.Status != DurableActionStatus.Locked || action.ConcurrencyToken != expectedConcurrencyToken)
            return MuleActionTransitionOutcome.LeaseLost;

        transition(action);
        action.ConcurrencyToken = Ulid.NewUlid();
        try
        {
            await _dbContext.SaveChangesAsync(cancellationToken);
            return MuleActionTransitionOutcome.Applied;
        }
        catch (DbUpdateConcurrencyException)
        {
            DetachChangedEntries();
            return MuleActionTransitionOutcome.LeaseLost;
        }
    }

    public void Dispose()
        => _dbContext.Dispose();

    public ValueTask DisposeAsync()
        => _dbContext.DisposeAsync();

    private void DetachChangedEntries()
    {
        foreach (var entry in _dbContext.ChangeTracker.Entries<DurableAction>().Where(x => x.State != EntityState.Unchanged))
            entry.State = EntityState.Detached;
    }

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;

    private static bool IsUniqueConstraintViolation(DbUpdateException exception)
        => exception.InnerException?.Message.Contains("unique", StringComparison.OrdinalIgnoreCase) == true ||
           exception.InnerException?.Message.Contains("duplicate", StringComparison.OrdinalIgnoreCase) == true;

    private static bool ShouldFallbackToClientFiltering(Exception exception)
        => exception is InvalidOperationException or NotSupportedException;

    private static bool CanLock(
        DurableActionStatus status,
        DateTimeOffset? lockedOnUtc,
        DateTimeOffset? nextAttemptOnUtc,
        DateTimeOffset lockExpiration,
        DateTimeOffset now)
        => status switch
        {
            DurableActionStatus.Pending => nextAttemptOnUtc is not { } nextAttempt || nextAttempt <= now,
            DurableActionStatus.Locked => lockedOnUtc is { } lockedOn && lockedOn <= lockExpiration,
            _ => false
        };

    private static DateTimeOffset? GetNextPendingOnUtc(
        DurableActionStatus status,
        DateTimeOffset? lockedOnUtc,
        DateTimeOffset? nextAttemptOnUtc,
        DateTimeOffset lockExpiration,
        TimeSpan lockTimeout,
        DateTimeOffset now)
        => status switch
        {
            DurableActionStatus.Pending when nextAttemptOnUtc is not { } nextAttempt => now,
            DurableActionStatus.Pending when nextAttemptOnUtc is { } nextAttempt => nextAttempt <= now ? now : nextAttempt,
            DurableActionStatus.Locked when lockedOnUtc is not { } lockedOn => null,
            DurableActionStatus.Locked when lockedOnUtc is { } lockedOn => lockedOn <= lockExpiration ? now : lockedOn.Add(lockTimeout),
            _ => null
        };
}

internal sealed class EntityFrameworkMuleStorage : EntityFrameworkMuleStorage<MuleDbContext>
{
    public EntityFrameworkMuleStorage(IMuleDbContextFactory<MuleDbContext> dbContextFactory, MuleRuntimeMetrics metrics)
        : base(dbContextFactory, metrics)
    {
    }
}
