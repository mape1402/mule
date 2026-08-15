namespace Mule.InMemory;

using Mule.Diagnostics;

internal sealed class InMemoryMuleStore
{
    private readonly object _gate = new();
    private readonly List<DurableAction> _actions = new();
    private int _duplicatesIgnored;

    public IReadOnlyCollection<DurableAction> Actions
    {
        get
        {
            lock (_gate)
                return _actions.Select(Clone).ToArray();
        }
    }

    public void Add(DurableAction action)
    {
        lock (_gate)
        {
            if (!string.IsNullOrWhiteSpace(action.DeduplicationKey) &&
                _actions.Any(x => x.Key == action.Key && x.DeduplicationKey == action.DeduplicationKey))
            {
                _duplicatesIgnored++;
                return;
            }

            _actions.Add(Clone(action));
        }
    }

    public IReadOnlyCollection<DurableAction> ClaimPending(string lane, int batchSize, TimeSpan lockTimeout, DateTimeOffset now)
    {
        lock (_gate)
        {
            var lockExpiration = now.Subtract(lockTimeout);
            var actions = _actions
                .Where(x =>
                    IsLane(x, lane) &&
                    (x.Status == DurableActionStatus.Pending && (x.NextAttemptOnUtc == null || x.NextAttemptOnUtc <= now) ||
                    x.Status == DurableActionStatus.Locked && x.LockedOnUtc <= lockExpiration))
                .OrderBy(x => x.CreatedOnUtc)
                .Take(batchSize)
                .ToArray();

            foreach (var action in actions)
            {
                action.Status = DurableActionStatus.Locked;
                action.LockedOnUtc = now;
                action.StartedOnUtc ??= now;
            }

            return actions
                .Select(Clone)
                .ToArray();
        }
    }

    public DurableAction Lock(Guid id, TimeSpan lockTimeout, DateTimeOffset now)
    {
        lock (_gate)
        {
            var action = _actions.SingleOrDefault(x => x.Id == id);

            if (action == null)
                return null;

            var lockExpiration = now.Subtract(lockTimeout);
            var canLock =
                action.Status == DurableActionStatus.Pending && (action.NextAttemptOnUtc == null || action.NextAttemptOnUtc <= now) ||
                action.Status == DurableActionStatus.Locked && action.LockedOnUtc <= lockExpiration;

            if (!canLock)
                return null;

            action.Status = DurableActionStatus.Locked;
            action.LockedOnUtc = now;
            action.StartedOnUtc ??= now;

            return Clone(action);
        }
    }

    public DateTimeOffset? GetNextPendingOnUtc(TimeSpan lockTimeout, DateTimeOffset now)
    {
        lock (_gate)
        {
            var lockExpiration = now.Subtract(lockTimeout);
            return _actions
                .Where(x => x.Status is DurableActionStatus.Pending or DurableActionStatus.Locked)
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
    }

    public void MarkCompleted(Guid id, DateTimeOffset completedOnUtc)
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
        }
    }

    public void MarkFailed(Guid id, string error, DateTimeOffset? nextAttemptOnUtc)
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

    public MuleDiagnosticsSnapshot GetSnapshot(TimeSpan lockTimeout)
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
                LockExpired = _actions.Count(x => x.Status == DurableActionStatus.Locked && x.LockedOnUtc <= lockExpiration),
                DuplicatesIgnored = _duplicatesIgnored,
                ThroughputPerMinute = _actions.Count(x => x.Status == DurableActionStatus.Completed && x.CompletedOnUtc >= throughputSinceUtc),
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
                    .ToDictionary(x => x.Key, x => x.Sum(action => action.Attempts))
            };
        }
    }

    internal static DurableAction Clone(DurableAction action)
        => new()
        {
            Id = action.Id,
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

    private static bool IsBacklog(DurableAction action)
        => action.Status is DurableActionStatus.Pending or DurableActionStatus.Locked;

    private static bool IsLane(DurableAction action, string lane)
        => string.Equals(NormalizeLane(action.Lane), NormalizeLane(lane), StringComparison.OrdinalIgnoreCase);

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;

    private static TimeSpan? AverageDuration(IEnumerable<DurableAction> actions, Func<DurableAction, TimeSpan> selector)
    {
        var values = actions.Select(selector).ToArray();
        if (values.Length == 0)
            return null;

        return TimeSpan.FromTicks((long)values.Average(x => x.Ticks));
    }

    private DurableAction Find(Guid id)
        => _actions.SingleOrDefault(x => x.Id == id)
            ?? throw new InvalidOperationException($"Mule action '{id}' was not found.");
}
