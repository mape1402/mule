namespace Mule.InMemory;

using Mule.Diagnostics;
using Mule.Replay;

internal sealed class InMemoryMuleStore
{
    private readonly object _gate = new();
    private readonly List<DurableAction> _actions = new();
    private readonly List<MuleReplay> _replays = new();

    public IReadOnlyCollection<DurableAction> Actions
    {
        get
        {
            lock (_gate)
                return _actions.Select(Clone).ToArray();
        }
    }

    public bool Add(DurableAction action)
    {
        lock (_gate)
        {
            if (_actions.Any(x => x.Id == action.Id))
                return false;

            if (!string.IsNullOrWhiteSpace(action.DeduplicationKey) &&
                _actions.Any(x => x.Key == action.Key && x.DeduplicationKey == action.DeduplicationKey))
                return false;

            _actions.Add(Clone(action));
            return true;
        }
    }

    public MuleCancellationResult Cancel(Ulid actionId, DateTimeOffset now)
    {
        lock (_gate)
        {
            var action = _actions.SingleOrDefault(x => x.Id == actionId);
            if (action != null)
            {
                if (action.Status == DurableActionStatus.Cancelled)
                    return Result(actionId, MuleCancellationOutcome.AlreadyCancelled, action.CancelledOnUtc);
                if (action.Status != DurableActionStatus.Pending)
                    return Result(actionId, MuleCancellationOutcome.TooLate);

                action.Status = DurableActionStatus.Cancelled;
                action.CancelledOnUtc = now;
                action.TerminalOnUtc = now;
                action.NextAttemptOnUtc = null;
                action.LockedOnUtc = null;
                action.ConcurrencyToken = Ulid.NewUlid();
                return Result(actionId, MuleCancellationOutcome.Cancelled, now);
            }

            var replay = _replays.SingleOrDefault(x => x.ReplayActionId == actionId);
            if (replay == null)
                return Result(actionId, MuleCancellationOutcome.NotFound);
            if (replay.Status == MuleReplayStatus.Cancelled)
                return Result(actionId, MuleCancellationOutcome.AlreadyCancelled, replay.CancelledOnUtc);
            if (replay.Status != MuleReplayStatus.Scheduled || replay.LockedOnUtc != null)
                return Result(actionId, MuleCancellationOutcome.TooLate);

            replay.Status = MuleReplayStatus.Cancelled;
            replay.CancelledOnUtc = now;
            replay.NextAttemptOnUtc = null;
            replay.ConcurrencyToken = Ulid.NewUlid();
            return Result(actionId, MuleCancellationOutcome.Cancelled, now);
        }
    }

    public bool MarkCancelled(Ulid actionId, DateTimeOffset now)
    {
        lock (_gate)
        {
            var action = _actions.SingleOrDefault(x => x.Id == actionId);
            if (action == null)
                return false;
            if (action.Status == DurableActionStatus.Cancelled)
                return true;
            if (action.Status is not (DurableActionStatus.Pending or DurableActionStatus.Locked))
                return false;

            action.Status = DurableActionStatus.Cancelled;
            action.CancelledOnUtc = now;
            action.TerminalOnUtc = now;
            action.NextAttemptOnUtc = null;
            action.LockedOnUtc = null;
            action.ConcurrencyToken = Ulid.NewUlid();
            return true;
        }
    }

    public MuleReplayResult ScheduleReplay(Ulid sourceActionId, MuleReplayRequest request, DateTimeOffset now)
    {
        lock (_gate)
        {
            var existingRequest = _replays.SingleOrDefault(x => x.RequestId == request.RequestId);
            if (existingRequest != null)
            {
                return Result(
                    MuleReplayModel.Matches(existingRequest, sourceActionId, request)
                        ? MuleReplayOutcome.AlreadyScheduled
                        : MuleReplayOutcome.RequestConflict,
                    existingRequest);
            }

            var source = _actions.SingleOrDefault(x => x.Id == sourceActionId);
            if (source == null)
                return Result(MuleReplayOutcome.NotFound, null);
            if (source.Status != DurableActionStatus.Failed)
                return Result(MuleReplayOutcome.NotEligible, null);

            var branchId = MuleReplayModel.GetBranchId(request);
            var existingBranch = _replays.SingleOrDefault(x =>
                x.SourceActionId == sourceActionId && x.BranchId == branchId);
            if (existingBranch != null)
                return Result(MuleReplayOutcome.ReplayAlreadyExists, existingBranch);

            var parent = _replays.SingleOrDefault(x => x.ReplayActionId == sourceActionId);
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
            _replays.Add(replay);
            return Result(MuleReplayOutcome.Scheduled, replay);
        }
    }

    public MuleReplayInfo FindReplay(Ulid requestId)
    {
        lock (_gate)
            return MuleReplayModel.ToInfo(_replays.SingleOrDefault(x => x.RequestId == requestId));
    }

    public IReadOnlyCollection<MuleReplayClaim> ClaimScheduledReplays(
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now)
    {
        lock (_gate)
        {
            var lockExpiration = now.Subtract(lockTimeout);
            var replays = _replays
                .Where(x => CanClaimReplay(x, lockExpiration, now))
                .OrderBy(x => x.ScheduleOnUtc)
                .ThenBy(x => x.RequestedOnUtc)
                .Take(Math.Max(1, batchSize))
                .ToArray();
            var claims = new List<MuleReplayClaim>(replays.Length);

            foreach (var replay in replays)
            {
                var source = _actions.SingleOrDefault(x => x.Id == replay.SourceActionId);
                if (source == null)
                    continue;

                replay.LockedOnUtc = now;
                replay.LockToken = Ulid.NewUlid();
                replay.ConcurrencyToken = Ulid.NewUlid();
                replay.Attempts++;
                claims.Add(new MuleReplayClaim
                {
                    Replay = Clone(replay),
                    SourceAction = Clone(source),
                    LockToken = replay.LockToken
                });
            }

            return claims;
        }
    }

    public DateTimeOffset? GetNextScheduledReplayOnUtc(TimeSpan lockTimeout, DateTimeOffset now)
    {
        lock (_gate)
        {
            var lockExpiration = now.Subtract(lockTimeout);
            return _replays
                .Where(x => x.Status == MuleReplayStatus.Scheduled)
                .Select(x => (DateTimeOffset?)GetNextReplayOnUtc(x, lockExpiration, lockTimeout, now))
                .OrderBy(x => x)
                .FirstOrDefault();
        }
    }

    public MuleReplayMaterializationOutcome MaterializeReplay(
        MuleReplayClaim claim,
        DurableAction action,
        DateTimeOffset now)
    {
        lock (_gate)
        {
            var replay = _replays.SingleOrDefault(x => x.RequestId == claim.Replay.RequestId);
            if (replay == null)
                return MuleReplayMaterializationOutcome.LeaseLost;

            var existingAction = _actions.SingleOrDefault(x => x.Id == replay.ReplayActionId);
            if (replay.Status == MuleReplayStatus.Materialized && existingAction != null)
                return MuleReplayMaterializationOutcome.Reconciled;
            if (replay.LockToken != claim.LockToken || replay.Status != MuleReplayStatus.Scheduled)
                return MuleReplayMaterializationOutcome.LeaseLost;

            var outcome = MuleReplayMaterializationOutcome.Reconciled;
            if (existingAction == null)
            {
                if (!AddWithoutLock(action))
                    throw new InvalidOperationException($"Mule replay action '{action.Id}' could not be persisted.");

                outcome = MuleReplayMaterializationOutcome.Created;
            }

            replay.Status = MuleReplayStatus.Materialized;
            replay.MaterializedOnUtc = now;
            replay.LockedOnUtc = null;
            replay.LockToken = Ulid.Empty;
            replay.NextAttemptOnUtc = null;
            replay.LastError = null;
            replay.ConcurrencyToken = Ulid.NewUlid();
            return outcome;
        }
    }

    public void ReleaseReplay(MuleReplayClaim claim, string error, DateTimeOffset nextAttemptOnUtc)
    {
        lock (_gate)
        {
            var replay = _replays.SingleOrDefault(x => x.RequestId == claim.Replay.RequestId);
            if (replay == null || replay.Status != MuleReplayStatus.Scheduled || replay.LockToken != claim.LockToken)
                return;

            replay.LockedOnUtc = null;
            replay.LockToken = Ulid.Empty;
            replay.LastError = error;
            replay.NextAttemptOnUtc = nextAttemptOnUtc;
            replay.ConcurrencyToken = Ulid.NewUlid();
        }
    }

    public Ulid? FindByDeduplicationKey(ActionKey key, string deduplicationKey)
    {
        if (string.IsNullOrWhiteSpace(deduplicationKey))
            return null;

        lock (_gate)
        {
            return _actions
                .Where(x => x.Key == key && x.DeduplicationKey == deduplicationKey)
                .Select(x => (Ulid?)x.Id)
                .FirstOrDefault();
        }
    }

    public IReadOnlyCollection<DurableAction> ClaimPending(string lane, int batchSize, TimeSpan lockTimeout, DateTimeOffset now)
    {
        lock (_gate)
        {
            var lockExpiration = now.Subtract(lockTimeout);
            var actions = _actions
                .Where(x => IsLane(x, lane) && CanLock(x, lockExpiration, now))
                .OrderBy(x => x.CreatedOnUtc)
                .Take(batchSize)
                .ToArray();

            foreach (var action in actions)
            {
                action.Status = DurableActionStatus.Locked;
                action.LockedOnUtc = now;
                action.StartedOnUtc ??= now;
                action.ConcurrencyToken = Ulid.NewUlid();
            }

            return actions
                .Select(Clone)
                .ToArray();
        }
    }

    public DurableAction Lock(Ulid id, TimeSpan lockTimeout, DateTimeOffset now)
    {
        lock (_gate)
        {
            var action = _actions.SingleOrDefault(x => x.Id == id);

            if (action == null)
                return null;

            var lockExpiration = now.Subtract(lockTimeout);
            if (!CanLock(action, lockExpiration, now))
                return null;

            action.Status = DurableActionStatus.Locked;
            action.LockedOnUtc = now;
            action.StartedOnUtc ??= now;
            action.ConcurrencyToken = Ulid.NewUlid();

            return Clone(action);
        }
    }

    public DateTimeOffset? GetNextPendingOnUtc(TimeSpan lockTimeout, DateTimeOffset now)
    {
        lock (_gate)
        {
            var lockExpiration = now.Subtract(lockTimeout);
            return _actions
                .Select(x => GetNextPendingOnUtc(x, lockExpiration, lockTimeout, now))
                .Where(x => x != null)
                .OrderBy(x => x)
                .FirstOrDefault();
        }
    }

    public void MarkCompleted(Ulid id, DateTimeOffset completedOnUtc)
    {
        lock (_gate)
        {
            var action = Find(id);
            action.Status = DurableActionStatus.Completed;
            action.CompletedOnUtc = completedOnUtc;
            action.TerminalOnUtc = completedOnUtc;
            action.LockedOnUtc = null;
            action.NextAttemptOnUtc = null;
            action.LastError = null;
            action.ConcurrencyToken = Ulid.NewUlid();
        }
    }

    public void MarkFailed(Ulid id, string error, DateTimeOffset? nextAttemptOnUtc)
    {
        lock (_gate)
        {
            var action = Find(id);
            action.Attempts++;
            action.Status = nextAttemptOnUtc == null ? DurableActionStatus.Failed : DurableActionStatus.Pending;
            action.LastError = error;
            action.LockedOnUtc = null;
            action.NextAttemptOnUtc = nextAttemptOnUtc;
            action.TerminalOnUtc = nextAttemptOnUtc == null ? DateTimeOffset.UtcNow : null;
            action.ConcurrencyToken = Ulid.NewUlid();
        }
    }

    public int CleanCompleted(DateTimeOffset olderThanUtc, int batchSize)
    {
        lock (_gate)
        {
            var actions = _actions
                .Where(x => x.Status == DurableActionStatus.Completed && x.CompletedOnUtc <= olderThanUtc)
                .OrderBy(x => x.CompletedOnUtc)
                .Take(batchSize)
                .ToArray();

            foreach (var action in actions)
                _actions.Remove(action);

            return actions.Length;
        }
    }

    public DateTimeOffset? GetOldestCompletedOnUtc()
    {
        lock (_gate)
        {
            return _actions
                .Where(x => x.Status == DurableActionStatus.Completed && x.CompletedOnUtc != null)
                .OrderBy(x => x.CompletedOnUtc)
                .Select(x => x.CompletedOnUtc)
                .FirstOrDefault();
        }
    }

    public MuleDiagnosticsSnapshot GetSnapshot(TimeSpan lockTimeout, MuleRuntimeMetricsSnapshot runtime)
    {
        lock (_gate)
        {
            var now = DateTimeOffset.UtcNow;
            var lockExpiration = now.Subtract(lockTimeout);
            var throughputSinceUtc = now.AddMinutes(-1);
            var oldestPendingOnUtc = _actions
                .Where(x => x.Status == DurableActionStatus.Pending)
                .OrderBy(x => x.CreatedOnUtc)
                .Select(x => (DateTimeOffset?)x.CreatedOnUtc)
                .FirstOrDefault();
            var oldestLockedOnUtc = _actions
                .Where(x => x.Status == DurableActionStatus.Locked)
                .OrderBy(x => x.LockedOnUtc)
                .Select(x => x.LockedOnUtc)
                .FirstOrDefault();

            return new MuleDiagnosticsSnapshot
            {
                Pending = _actions.Count(x => x.Status == DurableActionStatus.Pending),
                Locked = _actions.Count(x => x.Status == DurableActionStatus.Locked),
                Completed = _actions.Count(x => x.Status == DurableActionStatus.Completed),
                Failed = _actions.Count(x => x.Status == DurableActionStatus.Failed),
                Cancelled = _actions.Count(x => x.Status == DurableActionStatus.Cancelled),
                LockExpired = _actions.Count(x => x.Status == DurableActionStatus.Locked && x.LockedOnUtc <= lockExpiration),
                DuplicatesIgnored = runtime.DuplicatesIgnored,
                ThroughputPerMinute = Math.Max(
                    runtime.ThroughputPerMinute,
                    _actions.Count(x => x.Status == DurableActionStatus.Completed && x.CompletedOnUtc >= throughputSinceUtc)),
                RuntimeCompleted = runtime.Completed,
                RuntimeFailed = runtime.Failed,
                ReplayScheduled = runtime.ReplayScheduled,
                ReplayMaterialized = runtime.ReplayMaterialized,
                ReplayAlreadyScheduled = runtime.ReplayAlreadyScheduled,
                ReplayRejected = runtime.ReplayRejected,
                ReplayRetried = runtime.ReplayRetried,
                ReplayMaterializationFailures = runtime.ReplayMaterializationFailures,
                OldestPendingOnUtc = oldestPendingOnUtc,
                OldestLockedOnUtc = oldestLockedOnUtc,
                OldestFailedOnUtc = _actions
                    .Where(x => x.Status == DurableActionStatus.Failed)
                    .OrderBy(x => x.CreatedOnUtc)
                    .Select(x => (DateTimeOffset?)x.CreatedOnUtc)
                    .FirstOrDefault(),
                OldestPendingAge = oldestPendingOnUtc == null ? null : now - oldestPendingOnUtc.Value,
                OldestLockedAge = oldestLockedOnUtc == null ? null : now - oldestLockedOnUtc.Value,
                AverageEnqueueToExecutionLatency = AverageDuration(_actions.Where(x => x.StartedOnUtc != null), x => x.StartedOnUtc.Value - x.CreatedOnUtc),
                AverageExecutionLatency = AverageDuration(_actions.Where(x => x.StartedOnUtc != null && x.CompletedOnUtc != null), x => x.CompletedOnUtc.Value - x.StartedOnUtc.Value),
                AverageEnqueueToTerminalLatency = AverageDuration(_actions.Where(x => x.TerminalOnUtc != null), x => x.TerminalOnUtc.Value - x.CreatedOnUtc),
                BacklogByLane = _actions
                    .Where(IsBacklog)
                    .GroupBy(x => NormalizeLane(x.Lane))
                    .ToDictionary(x => x.Key, x => x.Count(), StringComparer.OrdinalIgnoreCase),
                BacklogByActionKey = _actions
                    .Where(IsBacklog)
                    .GroupBy(x => x.Key)
                    .ToDictionary(x => x.Key, x => x.Count()),
                FailuresByActionKey = _actions
                    .Where(x => x.Status == DurableActionStatus.Failed)
                    .GroupBy(x => x.Key)
                    .ToDictionary(x => x.Key, x => x.Count()),
                RetriesByActionKey = _actions
                    .Where(x => x.Attempts > 0)
                    .GroupBy(x => x.Key)
                    .ToDictionary(x => x.Key, x => x.Sum(action => action.Attempts)),
                CompletedPerMinuteByLane = runtime.CompletedPerMinuteByLane,
                CompletedPerMinuteByActionKey = runtime.CompletedPerMinuteByActionKey,
                RuntimeFailedByLane = runtime.RuntimeFailedByLane,
                RuntimeFailedByActionKey = runtime.RuntimeFailedByActionKey,
                ClaimedByLane = runtime.ClaimedByLane,
                WaitingExecutionByLane = runtime.WaitingExecutionByLane,
                ExecutingByLane = runtime.ExecutingByLane,
                ExecutorSaturationByLane = runtime.ExecutorSaturationByLane,
                OldestWaitingExecutionAgeByLane = runtime.OldestWaitingExecutionAgeByLane
            };
        }
    }

    internal static DurableAction Clone(DurableAction action)
        => new()
        {
            Id = action.Id,
            ConcurrencyToken = action.ConcurrencyToken,
            Key = action.Key,
            Lane = NormalizeLane(action.Lane),
            Payload = action.Payload,
            PayloadType = action.PayloadType,
            Metadata = action.Metadata,
            CorrelationId = action.CorrelationId,
            DeduplicationKey = action.DeduplicationKey,
            Status = action.Status,
            Attempts = action.Attempts,
            LastError = action.LastError,
            CreatedOnUtc = action.CreatedOnUtc,
            LockedOnUtc = action.LockedOnUtc,
            StartedOnUtc = action.StartedOnUtc,
            NextAttemptOnUtc = action.NextAttemptOnUtc,
            CompletedOnUtc = action.CompletedOnUtc,
            CancelledOnUtc = action.CancelledOnUtc,
            TerminalOnUtc = action.TerminalOnUtc
        };

    internal static MuleReplay Clone(MuleReplay replay)
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
            MaterializedOnUtc = replay.MaterializedOnUtc,
            CancelledOnUtc = replay.CancelledOnUtc
        };

    private static MuleCancellationResult Result(
        Ulid actionId,
        MuleCancellationOutcome outcome,
        DateTimeOffset? cancelledOnUtc = null)
        => new(actionId, outcome, cancelledOnUtc);

    private static bool IsBacklog(DurableAction action)
        => action.Status is DurableActionStatus.Pending or DurableActionStatus.Locked;

    private static bool IsLane(DurableAction action, string lane)
        => string.Equals(NormalizeLane(action.Lane), NormalizeLane(lane), StringComparison.OrdinalIgnoreCase);

    private static bool CanLock(DurableAction action, DateTimeOffset lockExpiration, DateTimeOffset now)
        => action.Status switch
        {
            DurableActionStatus.Pending => action.NextAttemptOnUtc is not { } nextAttemptOnUtc || nextAttemptOnUtc <= now,
            DurableActionStatus.Locked => action.LockedOnUtc is { } lockedOnUtc && lockedOnUtc <= lockExpiration,
            _ => false
        };

    private static bool CanClaimReplay(MuleReplay replay, DateTimeOffset lockExpiration, DateTimeOffset now)
        => replay.Status == MuleReplayStatus.Scheduled
            && replay.ScheduleOnUtc <= now
            && (!replay.NextAttemptOnUtc.HasValue || replay.NextAttemptOnUtc.Value <= now)
            && (!replay.LockedOnUtc.HasValue || replay.LockedOnUtc.Value <= lockExpiration);

    private static DateTimeOffset GetNextReplayOnUtc(
        MuleReplay replay,
        DateTimeOffset lockExpiration,
        TimeSpan lockTimeout,
        DateTimeOffset now)
    {
        var dueOnUtc = replay.ScheduleOnUtc;
        if (replay.NextAttemptOnUtc is { } nextAttemptOnUtc && nextAttemptOnUtc > dueOnUtc)
            dueOnUtc = nextAttemptOnUtc;
        if (replay.LockedOnUtc is { } lockedOnUtc && lockedOnUtc > lockExpiration)
        {
            var lockDueOnUtc = lockedOnUtc.Add(lockTimeout);
            if (lockDueOnUtc > dueOnUtc)
                dueOnUtc = lockDueOnUtc;
        }

        return dueOnUtc <= now ? now : dueOnUtc;
    }

    private static MuleReplayResult Result(MuleReplayOutcome outcome, MuleReplay replay)
        => new()
        {
            Outcome = outcome,
            Replay = MuleReplayModel.ToInfo(replay)
        };

    private bool AddWithoutLock(DurableAction action)
    {
        if (_actions.Any(x => x.Id == action.Id))
            return false;
        if (!string.IsNullOrWhiteSpace(action.DeduplicationKey)
            && _actions.Any(x => x.Key == action.Key && x.DeduplicationKey == action.DeduplicationKey))
            return false;

        _actions.Add(Clone(action));
        return true;
    }

    private static DateTimeOffset? GetNextPendingOnUtc(
        DurableAction action,
        DateTimeOffset lockExpiration,
        TimeSpan lockTimeout,
        DateTimeOffset now)
        => action.Status switch
        {
            DurableActionStatus.Pending when action.NextAttemptOnUtc is not { } nextAttemptOnUtc => now,
            DurableActionStatus.Pending when action.NextAttemptOnUtc is { } nextAttemptOnUtc => nextAttemptOnUtc <= now ? now : nextAttemptOnUtc,
            DurableActionStatus.Locked when action.LockedOnUtc is not { } lockedOnUtc => null,
            DurableActionStatus.Locked when action.LockedOnUtc is { } lockedOnUtc => lockedOnUtc <= lockExpiration ? now : lockedOnUtc.Add(lockTimeout),
            _ => null
        };

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;

    private static TimeSpan? AverageDuration(IEnumerable<DurableAction> actions, Func<DurableAction, TimeSpan> selector)
    {
        var values = actions.Select(selector).ToArray();
        if (values.Length == 0)
            return null;

        return TimeSpan.FromTicks((long)values.Average(x => x.Ticks));
    }

    private DurableAction Find(Ulid id)
        => _actions.SingleOrDefault(x => x.Id == id)
            ?? throw new InvalidOperationException($"Mule action '{id}' was not found.");
}
