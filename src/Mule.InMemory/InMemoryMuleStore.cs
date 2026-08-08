namespace Mule.InMemory;

using Mule.Diagnostics;

internal sealed class InMemoryMuleStore
{
    private readonly object _gate = new();
    private readonly List<DurableAction> _actions = new();

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
                return;

            _actions.Add(Clone(action));
        }
    }

    public IReadOnlyCollection<DurableAction> GetPending(int batchSize, TimeSpan lockTimeout, DateTimeOffset now)
    {
        lock (_gate)
        {
            var lockExpiration = now.Subtract(lockTimeout);
            return _actions
                .Where(x =>
                    x.Status == DurableActionStatus.Pending && (x.NextAttemptOnUtc == null || x.NextAttemptOnUtc <= now) ||
                    x.Status == DurableActionStatus.Locked && x.LockedOnUtc <= lockExpiration)
                .OrderBy(x => x.CreatedOnUtc)
                .Take(batchSize)
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

            return Clone(action);
        }
    }

    public void MarkCompleted(Guid id, DateTimeOffset completedOnUtc)
    {
        lock (_gate)
        {
            var action = Find(id);
            action.Status = DurableActionStatus.Completed;
            action.CompletedOnUtc = completedOnUtc;
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

    public MuleDiagnosticsSnapshot GetSnapshot()
    {
        lock (_gate)
        {
            return new MuleDiagnosticsSnapshot
            {
                Pending = _actions.Count(x => x.Status == DurableActionStatus.Pending),
                Locked = _actions.Count(x => x.Status == DurableActionStatus.Locked),
                Completed = _actions.Count(x => x.Status == DurableActionStatus.Completed),
                Failed = _actions.Count(x => x.Status == DurableActionStatus.Failed),
                OldestPendingOnUtc = _actions
                    .Where(x => x.Status == DurableActionStatus.Pending)
                    .OrderBy(x => x.CreatedOnUtc)
                    .Select(x => (DateTimeOffset?)x.CreatedOnUtc)
                    .FirstOrDefault(),
                OldestFailedOnUtc = _actions
                    .Where(x => x.Status == DurableActionStatus.Failed)
                    .OrderBy(x => x.CreatedOnUtc)
                    .Select(x => (DateTimeOffset?)x.CreatedOnUtc)
                    .FirstOrDefault()
            };
        }
    }

    internal static DurableAction Clone(DurableAction action)
        => new()
        {
            Id = action.Id,
            Key = action.Key,
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
            NextAttemptOnUtc = action.NextAttemptOnUtc,
            CompletedOnUtc = action.CompletedOnUtc
        };

    private DurableAction Find(Guid id)
        => _actions.SingleOrDefault(x => x.Id == id)
            ?? throw new InvalidOperationException($"Mule action '{id}' was not found.");
}
