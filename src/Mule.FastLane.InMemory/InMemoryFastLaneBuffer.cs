namespace Mule.FastLane.InMemory;

using System.Collections.Concurrent;

internal sealed class InMemoryFastLaneBuffer
{
    private readonly ConcurrentDictionary<Ulid, BufferedAction> _actions = new();
    private readonly object _gate = new();

    public bool TryAdd(DurableAction action)
    {
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        lock (_gate)
        {
            if (!string.IsNullOrWhiteSpace(action.DeduplicationKey) &&
                _actions.Values.Any(x =>
                    x.Action.Key == action.Key &&
                    x.Action.DeduplicationKey == action.DeduplicationKey &&
                    !x.TerminalFlushed))
                return false;

            return _actions.TryAdd(action.Id, new BufferedAction(CloneForBuffer(action)));
        }
    }

    public bool TryFindByDeduplicationKey(ActionKey key, string deduplicationKey, out Ulid id)
    {
        lock (_gate)
        {
            var match = _actions.Values.FirstOrDefault(x =>
                x.Action.Key == key &&
                x.Action.DeduplicationKey == deduplicationKey &&
                !x.TerminalFlushed);

            id = match?.Action.Id ?? Ulid.Empty;
            return match != null;
        }
    }

    public DurableAction Lock(Ulid id, TimeSpan lockTimeout, DateTimeOffset now)
    {
        lock (_gate)
        {
            if (!_actions.TryGetValue(id, out var buffered))
                return null;

            if (!CanLock(buffered.Action, lockTimeout, now))
                return null;

            buffered.Action.Status = DurableActionStatus.Locked;
            buffered.Action.LockedOnUtc = now;
            buffered.Action.StartedOnUtc ??= now;
            buffered.Action.PendingReason = DurableActionPendingReason.None;
            buffered.Action.DeferralReasonCode = null;
            buffered.Action.ConcurrencyToken = Ulid.NewUlid();
            buffered.IntentDirty = true;
            return CloneForExecution(buffered.Action);
        }
    }

    public IReadOnlyCollection<DurableAction> ClaimPending(
        string lane,
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now)
    {
        lane = NormalizeLane(lane);
        lock (_gate)
        {
            var claimed = _actions.Values
                .Where(x => x.Action.Lane == lane && CanLock(x.Action, lockTimeout, now))
                .OrderBy(x => x.Action.NextAttemptOnUtc ?? x.Action.CreatedOnUtc)
                .ThenBy(x => x.Action.CreatedOnUtc)
                .Take(batchSize)
                .ToArray();

            foreach (var buffered in claimed)
            {
                buffered.Action.Status = DurableActionStatus.Locked;
                buffered.Action.LockedOnUtc = now;
                buffered.Action.StartedOnUtc ??= now;
                buffered.Action.PendingReason = DurableActionPendingReason.None;
                buffered.Action.DeferralReasonCode = null;
                buffered.Action.ConcurrencyToken = Ulid.NewUlid();
                buffered.IntentDirty = true;
            }

            return claimed
                .Select(x => CloneForExecution(x.Action))
                .ToArray();
        }
    }

    public DateTimeOffset? GetNextPendingOnUtc(TimeSpan lockTimeout, DateTimeOffset now)
    {
        lock (_gate)
        {
            var lockExpiration = now.Subtract(lockTimeout);
            return _actions.Values
                .Select(x => GetNextPendingOnUtc(x.Action, lockExpiration, now))
                .Where(x => x != null)
                .OrderBy(x => x)
                .FirstOrDefault();
        }
    }

    public bool MarkCompleted(Ulid id, DateTimeOffset completedOnUtc)
    {
        lock (_gate)
        {
            if (!_actions.TryGetValue(id, out var buffered))
                return false;

            buffered.Action.Status = DurableActionStatus.Completed;
            buffered.Action.CompletedOnUtc = completedOnUtc;
            buffered.Action.TerminalOnUtc = completedOnUtc;
            buffered.Action.LockedOnUtc = null;
            buffered.Action.NextAttemptOnUtc = null;
            buffered.Action.PendingReason = DurableActionPendingReason.None;
            buffered.Action.DeferralReasonCode = null;
            buffered.Action.LastError = null;
            buffered.Action.ConcurrencyToken = Ulid.NewUlid();
            buffered.IntentDirty = true;
            buffered.TerminalDirty = true;
            return true;
        }
    }

    public bool MarkFailed(Ulid id, string error, DateTimeOffset now, DateTimeOffset? nextAttemptOnUtc)
    {
        lock (_gate)
        {
            if (!_actions.TryGetValue(id, out var buffered))
                return false;

            buffered.Action.Attempts++;
            buffered.Action.Status = nextAttemptOnUtc == null ? DurableActionStatus.Failed : DurableActionStatus.Pending;
            buffered.Action.LastError = error;
            buffered.Action.LockedOnUtc = null;
            buffered.Action.NextAttemptOnUtc = nextAttemptOnUtc;
            buffered.Action.PendingReason = nextAttemptOnUtc == null
                ? DurableActionPendingReason.None
                : DurableActionPendingReason.Retry;
            buffered.Action.DeferralReasonCode = null;
            buffered.Action.TerminalOnUtc = nextAttemptOnUtc == null ? now : null;
            buffered.Action.ConcurrencyToken = Ulid.NewUlid();
            buffered.IntentDirty = true;
            buffered.TerminalDirty = nextAttemptOnUtc == null;
            return true;
        }
    }

    public IReadOnlyCollection<DurableAction> ReadPendingCandidates(
        string lane,
        int offset,
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now)
    {
        lane = NormalizeLane(lane);
        lock (_gate)
        {
            return _actions.Values
                .Where(x => x.Action.Lane == lane && CanLock(x.Action, lockTimeout, now))
                .OrderBy(x => x.Action.NextAttemptOnUtc ?? x.Action.CreatedOnUtc)
                .ThenBy(x => x.Action.CreatedOnUtc)
                .ThenBy(x => x.Action.Id)
                .Skip(Math.Max(0, offset))
                .Take(Math.Max(1, batchSize))
                .Select(x => CloneForExecution(x.Action))
                .ToArray();
        }
    }

    public DurableAction FindAction(Ulid id)
    {
        lock (_gate)
            return _actions.TryGetValue(id, out var buffered)
                ? CloneForExecution(buffered.Action)
                : null;
    }

    public MuleActionTransitionOutcome CompleteClaim(Ulid id, Ulid expectedToken, DateTimeOffset completedOnUtc)
    {
        lock (_gate)
        {
            if (!_actions.TryGetValue(id, out var buffered))
                return MuleActionTransitionOutcome.NotFound;
            if (buffered.Action.Status != DurableActionStatus.Locked
                || buffered.Action.ConcurrencyToken != expectedToken)
                return MuleActionTransitionOutcome.LeaseLost;

            MarkCompletedCore(buffered, completedOnUtc);
            return MuleActionTransitionOutcome.Applied;
        }
    }

    public MuleActionTransitionOutcome FailClaim(
        Ulid id,
        Ulid expectedToken,
        string error,
        DateTimeOffset now,
        DateTimeOffset? nextAttemptOnUtc)
    {
        lock (_gate)
        {
            if (!_actions.TryGetValue(id, out var buffered))
                return MuleActionTransitionOutcome.NotFound;
            if (buffered.Action.Status != DurableActionStatus.Locked
                || buffered.Action.ConcurrencyToken != expectedToken)
                return MuleActionTransitionOutcome.LeaseLost;

            MarkFailedCore(buffered, error, now, nextAttemptOnUtc);
            return MuleActionTransitionOutcome.Applied;
        }
    }

    public MuleActionTransitionOutcome DeferClaim(
        Ulid id,
        Ulid expectedToken,
        DateTimeOffset notBeforeUtc,
        DeferralReason reason)
    {
        lock (_gate)
        {
            if (!_actions.TryGetValue(id, out var buffered))
                return MuleActionTransitionOutcome.NotFound;
            if (buffered.Action.Status != DurableActionStatus.Locked
                || buffered.Action.ConcurrencyToken != expectedToken)
                return MuleActionTransitionOutcome.LeaseLost;

            buffered.Action.Status = DurableActionStatus.Pending;
            buffered.Action.LockedOnUtc = null;
            buffered.Action.NextAttemptOnUtc = notBeforeUtc;
            buffered.Action.PendingReason = DurableActionPendingReason.Deferred;
            buffered.Action.DeferralReasonCode = reason.Value;
            buffered.Action.TerminalOnUtc = null;
            buffered.Action.ConcurrencyToken = Ulid.NewUlid();
            buffered.IntentDirty = true;
            buffered.TerminalDirty = false;
            return MuleActionTransitionOutcome.Applied;
        }
    }

    public bool TryCancel(Ulid id, DateTimeOffset cancelledOnUtc, out MuleCancellationResult result)
    {
        lock (_gate)
        {
            if (!_actions.TryGetValue(id, out var buffered))
            {
                result = null;
                return false;
            }

            if (buffered.Action.Status == DurableActionStatus.Cancelled)
            {
                result = new MuleCancellationResult(
                    id,
                    MuleCancellationOutcome.AlreadyCancelled,
                    buffered.Action.CancelledOnUtc);
                return true;
            }

            if (buffered.Action.Status != DurableActionStatus.Pending)
            {
                result = new MuleCancellationResult(id, MuleCancellationOutcome.TooLate);
                return true;
            }

            buffered.Action.Status = DurableActionStatus.Cancelled;
            buffered.Action.CancelledOnUtc = cancelledOnUtc;
            buffered.Action.TerminalOnUtc = cancelledOnUtc;
            buffered.Action.LockedOnUtc = null;
            buffered.Action.NextAttemptOnUtc = null;
            buffered.Action.ConcurrencyToken = Ulid.NewUlid();
            buffered.IntentDirty = true;
            buffered.TerminalDirty = true;
            buffered.TerminalFlushed = false;
            result = new MuleCancellationResult(id, MuleCancellationOutcome.Cancelled, cancelledOnUtc);
            return true;
        }
    }

    public IReadOnlyCollection<MuleActionStateProjection> TakeIntentFlushBatch(int batchSize)
    {
        lock (_gate)
        {
            return _actions.Values
                .Where(x => !x.IntentFlushing && (!x.IntentPersisted || x.IntentDirty))
                .OrderBy(x => x.Action.CreatedOnUtc)
                .Take(batchSize)
                .Select(x =>
                {
                    x.IntentFlushing = true;
                    x.IntentFlushToken = Ulid.NewUlid();
                    return new MuleActionStateProjection(
                        CloneForExecution(x.Action),
                        x.PersistedConcurrencyToken,
                        x.IntentFlushToken);
                })
                .ToArray();
        }
    }

    public void CompleteIntentFlush(IReadOnlyCollection<MuleActionStateProjection> projections)
    {
        lock (_gate)
        {
            foreach (var projection in projections)
            {
                if (!_actions.TryGetValue(projection.Action.Id, out var buffered))
                    continue;
                if (buffered.IntentFlushToken != projection.ProjectionToken)
                    continue;
                if (buffered.PersistedConcurrencyToken != projection.ExpectedConcurrencyToken)
                    continue;

                buffered.IntentPersisted = true;
                buffered.PersistedConcurrencyToken = projection.Action.ConcurrencyToken;
                buffered.IntentDirty = buffered.Action.ConcurrencyToken != projection.Action.ConcurrencyToken;
                buffered.IntentFlushing = false;
                buffered.IntentFlushToken = Ulid.Empty;
                if (!buffered.IntentDirty && buffered.Action.Status is
                    DurableActionStatus.Completed or DurableActionStatus.Failed or DurableActionStatus.Cancelled)
                {
                    buffered.TerminalDirty = false;
                    buffered.TerminalFlushed = true;
                }
                TryRemove(buffered);
            }
        }
    }

    public void CancelIntentFlush(IReadOnlyCollection<MuleActionStateProjection> projections)
    {
        lock (_gate)
        {
            foreach (var projection in projections)
            {
                if (_actions.TryGetValue(projection.Action.Id, out var buffered) &&
                    buffered.IntentFlushToken == projection.ProjectionToken)
                {
                    buffered.IntentFlushing = false;
                    buffered.IntentFlushToken = Ulid.Empty;
                }
            }
        }
    }

    public bool Contains(Ulid id)
        => _actions.ContainsKey(id);

    private void TryRemove(BufferedAction buffered)
    {
        if (buffered.IntentPersisted && !buffered.IntentDirty && buffered.TerminalFlushed && !buffered.TerminalDirty)
            _actions.TryRemove(buffered.Action.Id, out _);
    }

    private static bool CanLock(DurableAction action, TimeSpan lockTimeout, DateTimeOffset now)
    {
        var lockExpiration = now.Subtract(lockTimeout);
        return action.Status switch
        {
            DurableActionStatus.Pending => action.NextAttemptOnUtc is not { } nextAttemptOnUtc || nextAttemptOnUtc <= now,
            DurableActionStatus.Locked => action.LockedOnUtc is { } lockedOnUtc && lockedOnUtc <= lockExpiration,
            _ => false
        };
    }

    private static DateTimeOffset? GetNextPendingOnUtc(
        DurableAction action,
        DateTimeOffset lockExpiration,
        DateTimeOffset now)
        => action.Status switch
        {
            DurableActionStatus.Pending when action.NextAttemptOnUtc is not { } nextAttemptOnUtc => now,
            DurableActionStatus.Pending when action.NextAttemptOnUtc is { } nextAttemptOnUtc => nextAttemptOnUtc <= now ? now : nextAttemptOnUtc,
            DurableActionStatus.Locked when action.LockedOnUtc is { } lockedOnUtc && lockedOnUtc <= lockExpiration => now,
            _ => null
        };

    private static DurableAction CloneForBuffer(DurableAction action)
        => Clone(action);

    private static DurableAction CloneForExecution(DurableAction action)
        => Clone(action);

    private static DurableAction Clone(DurableAction action)
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
            PendingReason = action.PendingReason,
            DeferralReasonCode = action.DeferralReasonCode,
            CompletedOnUtc = action.CompletedOnUtc,
            CancelledOnUtc = action.CancelledOnUtc,
            TerminalOnUtc = action.TerminalOnUtc
        };

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;

    private static void MarkCompletedCore(BufferedAction buffered, DateTimeOffset completedOnUtc)
    {
        buffered.Action.Status = DurableActionStatus.Completed;
        buffered.Action.CompletedOnUtc = completedOnUtc;
        buffered.Action.TerminalOnUtc = completedOnUtc;
        buffered.Action.LockedOnUtc = null;
        buffered.Action.NextAttemptOnUtc = null;
        buffered.Action.PendingReason = DurableActionPendingReason.None;
        buffered.Action.DeferralReasonCode = null;
        buffered.Action.LastError = null;
        buffered.Action.ConcurrencyToken = Ulid.NewUlid();
        buffered.IntentDirty = true;
        buffered.TerminalDirty = true;
    }

    private static void MarkFailedCore(
        BufferedAction buffered,
        string error,
        DateTimeOffset now,
        DateTimeOffset? nextAttemptOnUtc)
    {
        buffered.Action.Attempts++;
        buffered.Action.Status = nextAttemptOnUtc == null ? DurableActionStatus.Failed : DurableActionStatus.Pending;
        buffered.Action.LastError = error;
        buffered.Action.LockedOnUtc = null;
        buffered.Action.NextAttemptOnUtc = nextAttemptOnUtc;
        buffered.Action.PendingReason = nextAttemptOnUtc == null
            ? DurableActionPendingReason.None
            : DurableActionPendingReason.Retry;
        buffered.Action.DeferralReasonCode = null;
        buffered.Action.TerminalOnUtc = nextAttemptOnUtc == null ? now : null;
        buffered.Action.ConcurrencyToken = Ulid.NewUlid();
        buffered.IntentDirty = true;
        buffered.TerminalDirty = nextAttemptOnUtc == null;
    }

    private sealed class BufferedAction
    {
        public BufferedAction(DurableAction action)
        {
            Action = action;
        }

        public DurableAction Action { get; }

        public bool IntentPersisted { get; set; }

        public Ulid? PersistedConcurrencyToken { get; set; }

        public bool IntentDirty { get; set; } = true;

        public bool IntentFlushing { get; set; }

        public Ulid IntentFlushToken { get; set; }

        public bool TerminalDirty { get; set; }

        public bool TerminalFlushed { get; set; }

    }
}
