namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Mule.Replay;

internal partial class EntityFrameworkMuleStorage<TDbContext>
{
    public async Task<MuleReplayResult> ScheduleAsync(
        Ulid sourceActionId,
        MuleReplayRequest request,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var existingRequest = await Replays
            .AsNoTracking()
            .SingleOrDefaultAsync(x => x.RequestId == request.RequestId, cancellationToken);
        if (existingRequest != null)
            return ExistingRequestResult(existingRequest, sourceActionId, request);

        var source = await Actions
            .AsNoTracking()
            .SingleOrDefaultAsync(x => x.Id == sourceActionId, cancellationToken);
        if (source == null)
            return Result(MuleReplayOutcome.NotFound, null);
        if (source.Status != DurableActionStatus.Failed)
            return Result(MuleReplayOutcome.NotEligible, null);

        var branchId = MuleReplayModel.GetBranchId(request);
        var existingBranch = await Replays
            .AsNoTracking()
            .SingleOrDefaultAsync(
                x => x.SourceActionId == sourceActionId && x.BranchId == branchId,
                cancellationToken);
        if (existingBranch != null)
            return Result(MuleReplayOutcome.ReplayAlreadyExists, existingBranch);

        var parent = await Replays
            .AsNoTracking()
            .SingleOrDefaultAsync(x => x.ReplayActionId == sourceActionId, cancellationToken);
        var replay = new MuleReplay
        {
            RequestId = request.RequestId,
            ConcurrencyToken = Ulid.NewUlid(),
            SourceActionId = sourceActionId,
            ReplayRootActionId = parent?.ReplayRootActionId ?? sourceActionId,
            ReplayActionId = Ulid.NewUlid(),
            ReplaySequence = (parent?.ReplaySequence ?? 0) + 1,
            BranchId = branchId,
            ActionKey = source.Key,
            Lane = source.Lane,
            CorrelationId = source.CorrelationId,
            OriginalDeduplicationKey = source.DeduplicationKey,
            Status = MuleReplayStatus.Scheduled,
            RequestedBy = request.RequestedBy,
            Reason = request.Reason,
            Metadata = MuleReplayModel.SerializeMetadata(request.Metadata),
            RequestedOnUtc = now,
            ScheduleOnUtc = request.ScheduleOnUtc ?? now
        };

        await Replays.AddAsync(replay, cancellationToken);
        try
        {
            await _dbContext.SaveChangesAsync(cancellationToken);
            return Result(MuleReplayOutcome.Scheduled, replay);
        }
        catch (DbUpdateException)
        {
            _dbContext.ChangeTracker.Clear();
            existingRequest = await Replays
                .AsNoTracking()
                .SingleOrDefaultAsync(x => x.RequestId == request.RequestId, cancellationToken);
            if (existingRequest != null)
                return ExistingRequestResult(existingRequest, sourceActionId, request);

            existingBranch = await Replays
                .AsNoTracking()
                .SingleOrDefaultAsync(
                    x => x.SourceActionId == sourceActionId && x.BranchId == branchId,
                    cancellationToken);
            if (existingBranch != null)
                return Result(MuleReplayOutcome.ReplayAlreadyExists, existingBranch);

            throw;
        }
    }

    async Task<MuleReplayInfo> IMuleReplayStorage.FindAsync(Ulid requestId, CancellationToken cancellationToken)
        => MuleReplayModel.ToInfo(await Replays
            .AsNoTracking()
            .SingleOrDefaultAsync(x => x.RequestId == requestId, cancellationToken));

    public async Task<IReadOnlyCollection<MuleReplayClaim>> ClaimScheduledAsync(
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var lockExpiration = now.Subtract(lockTimeout);
        var candidateIds = await GetReplayClaimCandidateIdsAsync(
            Math.Max(1, batchSize),
            lockExpiration,
            now,
            cancellationToken);
        var claims = new List<MuleReplayClaim>(candidateIds.Length);

        foreach (var requestId in candidateIds)
        {
            var claim = await TryClaimAsync(requestId, lockExpiration, now, cancellationToken);
            if (claim != null)
                claims.Add(claim);
        }

        return claims;
    }

    public async Task<DateTimeOffset?> GetNextScheduledOnUtcAsync(
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var lockExpiration = now.Subtract(lockTimeout);
        var candidates = await Replays
            .AsNoTracking()
            .Where(x => x.Status == MuleReplayStatus.Scheduled)
            .Select(x => new
            {
                x.ScheduleOnUtc,
                x.NextAttemptOnUtc,
                x.LockedOnUtc
            })
            .ToListAsync(cancellationToken);

        return candidates
            .Select(x => (DateTimeOffset?)GetNextReplayOnUtc(
                x.ScheduleOnUtc,
                x.NextAttemptOnUtc,
                x.LockedOnUtc,
                lockExpiration,
                lockTimeout,
                now))
            .OrderBy(x => x)
            .FirstOrDefault();
    }

    public async Task<MuleReplayMaterializationOutcome> MaterializeAsync(
        MuleReplayClaim claim,
        DurableAction action,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        if (claim == null)
            throw new ArgumentNullException(nameof(claim));
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        var replay = await Replays.SingleOrDefaultAsync(
            x => x.RequestId == claim.Replay.RequestId,
            cancellationToken);
        if (replay == null)
            return MuleReplayMaterializationOutcome.LeaseLost;

        var actionExists = await Actions.AsNoTracking().AnyAsync(x => x.Id == replay.ReplayActionId, cancellationToken);
        if (replay.Status == MuleReplayStatus.Materialized && actionExists)
            return MuleReplayMaterializationOutcome.Reconciled;
        if (replay.Status != MuleReplayStatus.Scheduled || replay.LockToken != claim.LockToken)
            return MuleReplayMaterializationOutcome.LeaseLost;

        var outcome = MuleReplayMaterializationOutcome.Reconciled;
        if (!actionExists)
        {
            await Actions.AddAsync(action, cancellationToken);
            try
            {
                await _dbContext.SaveChangesAsync(cancellationToken);
                outcome = MuleReplayMaterializationOutcome.Created;
            }
            catch (DbUpdateException)
            {
                _dbContext.Entry(action).State = EntityState.Detached;
                actionExists = await Actions.AsNoTracking().AnyAsync(x => x.Id == action.Id, cancellationToken);
                if (!actionExists)
                    throw;
            }
        }

        replay.Status = MuleReplayStatus.Materialized;
        replay.MaterializedOnUtc = now;
        replay.LockedOnUtc = null;
        replay.LockToken = Ulid.Empty;
        replay.NextAttemptOnUtc = null;
        replay.LastError = null;
        replay.ConcurrencyToken = Ulid.NewUlid();
        try
        {
            await _dbContext.SaveChangesAsync(cancellationToken);
            return outcome;
        }
        catch (DbUpdateConcurrencyException)
        {
            _dbContext.ChangeTracker.Clear();
            var persisted = await Replays
                .AsNoTracking()
                .SingleOrDefaultAsync(x => x.RequestId == claim.Replay.RequestId, cancellationToken);
            if (persisted != null && persisted.Status == MuleReplayStatus.Materialized)
                return MuleReplayMaterializationOutcome.Reconciled;

            throw;
        }
    }

    public async Task ReleaseAsync(
        MuleReplayClaim claim,
        string error,
        DateTimeOffset nextAttemptOnUtc,
        CancellationToken cancellationToken = default)
    {
        if (claim == null)
            throw new ArgumentNullException(nameof(claim));

        var replay = await Replays.SingleOrDefaultAsync(
            x => x.RequestId == claim.Replay.RequestId,
            cancellationToken);
        if (replay == null
            || replay.Status != MuleReplayStatus.Scheduled
            || replay.LockToken != claim.LockToken)
            return;

        replay.LockedOnUtc = null;
        replay.LockToken = Ulid.Empty;
        replay.LastError = error;
        replay.NextAttemptOnUtc = nextAttemptOnUtc;
        replay.ConcurrencyToken = Ulid.NewUlid();
        try
        {
            await _dbContext.SaveChangesAsync(cancellationToken);
        }
        catch (DbUpdateConcurrencyException)
        {
            _dbContext.ChangeTracker.Clear();
        }
    }

    private DbSet<MuleReplay> Replays => _dbContext.Set<MuleReplay>();

    private async Task<Ulid[]> GetReplayClaimCandidateIdsAsync(
        int batchSize,
        DateTimeOffset lockExpiration,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        try
        {
            return await Replays
                .AsNoTracking()
                .Where(x => x.Status == MuleReplayStatus.Scheduled
                    && x.ScheduleOnUtc <= now
                    && (!x.NextAttemptOnUtc.HasValue || x.NextAttemptOnUtc.Value <= now)
                    && (!x.LockedOnUtc.HasValue || x.LockedOnUtc.Value <= lockExpiration))
                .OrderBy(x => x.ScheduleOnUtc)
                .ThenBy(x => x.RequestedOnUtc)
                .Select(x => x.RequestId)
                .Take(batchSize)
                .ToArrayAsync(cancellationToken);
        }
        catch (Exception ex) when (ShouldFallbackToClientFiltering(ex))
        {
            var candidates = await Replays
                .AsNoTracking()
                .Where(x => x.Status == MuleReplayStatus.Scheduled)
                .Select(x => new
                {
                    x.RequestId,
                    x.ScheduleOnUtc,
                    x.NextAttemptOnUtc,
                    x.LockedOnUtc,
                    x.RequestedOnUtc
                })
                .ToListAsync(cancellationToken);

            return candidates
                .Where(x => x.ScheduleOnUtc <= now
                    && (!x.NextAttemptOnUtc.HasValue || x.NextAttemptOnUtc.Value <= now)
                    && (!x.LockedOnUtc.HasValue || x.LockedOnUtc.Value <= lockExpiration))
                .OrderBy(x => x.ScheduleOnUtc)
                .ThenBy(x => x.RequestedOnUtc)
                .Select(x => x.RequestId)
                .Take(batchSize)
                .ToArray();
        }
    }

    private async Task<MuleReplayClaim> TryClaimAsync(
        Ulid requestId,
        DateTimeOffset lockExpiration,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        var replay = await Replays.SingleOrDefaultAsync(x => x.RequestId == requestId, cancellationToken);
        if (replay == null || !CanClaimReplay(replay, lockExpiration, now))
            return null;

        var source = await Actions
            .AsNoTracking()
            .SingleOrDefaultAsync(x => x.Id == replay.SourceActionId, cancellationToken);
        if (source == null)
            return null;

        replay.LockedOnUtc = now;
        replay.LockToken = Ulid.NewUlid();
        replay.ConcurrencyToken = Ulid.NewUlid();
        replay.Attempts++;
        try
        {
            await _dbContext.SaveChangesAsync(cancellationToken);
            var claim = new MuleReplayClaim
            {
                Replay = CloneReplay(replay),
                SourceAction = source,
                LockToken = replay.LockToken
            };
            _dbContext.Entry(replay).State = EntityState.Detached;
            return claim;
        }
        catch (DbUpdateConcurrencyException)
        {
            _dbContext.Entry(replay).State = EntityState.Detached;
            return null;
        }
    }

    private static bool CanClaimReplay(MuleReplay replay, DateTimeOffset lockExpiration, DateTimeOffset now)
        => replay.Status == MuleReplayStatus.Scheduled
            && replay.ScheduleOnUtc <= now
            && (!replay.NextAttemptOnUtc.HasValue || replay.NextAttemptOnUtc.Value <= now)
            && (!replay.LockedOnUtc.HasValue || replay.LockedOnUtc.Value <= lockExpiration);

    private static DateTimeOffset GetNextReplayOnUtc(
        DateTimeOffset scheduleOnUtc,
        DateTimeOffset? nextAttemptOnUtc,
        DateTimeOffset? lockedOnUtc,
        DateTimeOffset lockExpiration,
        TimeSpan lockTimeout,
        DateTimeOffset now)
    {
        var dueOnUtc = scheduleOnUtc;
        if (nextAttemptOnUtc is { } nextAttempt && nextAttempt > dueOnUtc)
            dueOnUtc = nextAttempt;
        if (lockedOnUtc is { } lockedOn && lockedOn > lockExpiration)
        {
            var lockDueOnUtc = lockedOn.Add(lockTimeout);
            if (lockDueOnUtc > dueOnUtc)
                dueOnUtc = lockDueOnUtc;
        }

        return dueOnUtc <= now ? now : dueOnUtc;
    }

    private static MuleReplay CloneReplay(MuleReplay replay)
        => new()
        {
            RequestId = replay.RequestId,
            ConcurrencyToken = replay.ConcurrencyToken,
            SourceActionId = replay.SourceActionId,
            ReplayRootActionId = replay.ReplayRootActionId,
            ReplayActionId = replay.ReplayActionId,
            ReplaySequence = replay.ReplaySequence,
            BranchId = replay.BranchId,
            ActionKey = replay.ActionKey,
            Lane = replay.Lane,
            CorrelationId = replay.CorrelationId,
            OriginalDeduplicationKey = replay.OriginalDeduplicationKey,
            Status = replay.Status,
            RequestedBy = replay.RequestedBy,
            Reason = replay.Reason,
            Metadata = replay.Metadata,
            RequestedOnUtc = replay.RequestedOnUtc,
            ScheduleOnUtc = replay.ScheduleOnUtc,
            Attempts = replay.Attempts,
            NextAttemptOnUtc = replay.NextAttemptOnUtc,
            LastError = replay.LastError,
            LockedOnUtc = replay.LockedOnUtc,
            LockToken = replay.LockToken,
            MaterializedOnUtc = replay.MaterializedOnUtc
        };

    private static MuleReplayResult ExistingRequestResult(
        MuleReplay replay,
        Ulid sourceActionId,
        MuleReplayRequest request)
        => Result(
            MuleReplayModel.Matches(replay, sourceActionId, request)
                ? MuleReplayOutcome.AlreadyScheduled
                : MuleReplayOutcome.RequestConflict,
            replay);

    private static MuleReplayResult Result(MuleReplayOutcome outcome, MuleReplay replay)
        => new()
        {
            Outcome = outcome,
            Replay = MuleReplayModel.ToInfo(replay)
        };
}
