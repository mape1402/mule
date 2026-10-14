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
            buffered.Action.LastError = null;
            buffered.Action.ConcurrencyToken = Ulid.NewUlid();
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
            buffered.Action.TerminalOnUtc = nextAttemptOnUtc == null ? now : null;
            buffered.Action.ConcurrencyToken = Ulid.NewUlid();
            buffered.IntentDirty = nextAttemptOnUtc != null;
            buffered.TerminalDirty = nextAttemptOnUtc == null;
            return true;
        }
    }

    public IReadOnlyCollection<DurableAction> TakeIntentFlushBatch(int batchSize)
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
                    return CloneForDurableIntent(x.Action);
                })
                .ToArray();
        }
    }

    public void CompleteIntentFlush(IReadOnlyCollection<Ulid> ids)
    {
        lock (_gate)
        {
            foreach (var id in ids)
            {
                if (!_actions.TryGetValue(id, out var buffered))
                    continue;

                buffered.IntentPersisted = true;
                buffered.IntentDirty = false;
                buffered.IntentFlushing = false;
                TryRemove(buffered);
            }
        }
    }

    public void CancelIntentFlush(IReadOnlyCollection<Ulid> ids)
    {
        lock (_gate)
        {
            foreach (var id in ids)
            {
                if (_actions.TryGetValue(id, out var buffered))
                    buffered.IntentFlushing = false;
            }
        }
    }

    public IReadOnlyCollection<DurableAction> TakeTerminalFlushBatch(int batchSize)
    {
        lock (_gate)
        {
            return _actions.Values
                .Where(x => x.IntentPersisted && x.TerminalDirty && !x.TerminalFlushing)
                .OrderBy(x => x.Action.TerminalOnUtc ?? x.Action.CompletedOnUtc ?? x.Action.CreatedOnUtc)
                .Take(batchSize)
                .Select(x =>
                {
                    x.TerminalFlushing = true;
                    return CloneForExecution(x.Action);
                })
                .ToArray();
        }
    }

    public void CompleteTerminalFlush(IReadOnlyCollection<Ulid> ids)
    {
        lock (_gate)
        {
            foreach (var id in ids)
            {
                if (!_actions.TryGetValue(id, out var buffered))
                    continue;

                buffered.TerminalDirty = false;
                buffered.TerminalFlushed = true;
                buffered.TerminalFlushing = false;
                TryRemove(buffered);
            }
        }
    }

    public void CancelTerminalFlush(IReadOnlyCollection<Ulid> ids)
    {
        lock (_gate)
        {
            foreach (var id in ids)
            {
                if (_actions.TryGetValue(id, out var buffered))
                    buffered.TerminalFlushing = false;
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

    private static DurableAction CloneForDurableIntent(DurableAction action)
    {
        var clone = Clone(action);
        if (clone.Status is DurableActionStatus.Completed or DurableActionStatus.Failed)
        {
            clone.Status = DurableActionStatus.Locked;
            if (action.Status == DurableActionStatus.Failed)
                clone.Attempts = Math.Max(0, action.Attempts - 1);
            clone.CompletedOnUtc = null;
            clone.TerminalOnUtc = null;
            clone.LastError = null;
        }

        return clone;
    }

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
            CompletedOnUtc = action.CompletedOnUtc,
            TerminalOnUtc = action.TerminalOnUtc
        };

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;

    private sealed class BufferedAction
    {
        public BufferedAction(DurableAction action)
        {
            Action = action;
        }

        public DurableAction Action { get; }

        public bool IntentPersisted { get; set; }

        public bool IntentDirty { get; set; } = true;

        public bool IntentFlushing { get; set; }

        public bool TerminalDirty { get; set; }

        public bool TerminalFlushed { get; set; }

        public bool TerminalFlushing { get; set; }
    }
}
