namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Mule.Diagnostics;

internal class EntityFrameworkMuleDiagnostics<TDbContext> : IMuleDiagnostics, IDisposable, IAsyncDisposable
    where TDbContext : DbContext
{
    private readonly TDbContext _dbContext;
    private readonly MuleSettings _settings;

    public EntityFrameworkMuleDiagnostics(IMuleDbContextFactory<TDbContext> dbContextFactory, IOptions<MuleSettings> settings)
    {
        if (dbContextFactory == null)
            throw new ArgumentNullException(nameof(dbContextFactory));

        _dbContext = dbContextFactory.CreateDbContext();
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
    }

    public async Task<MuleDiagnosticsSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default)
    {
        var actions = _dbContext.Set<DurableAction>().AsNoTracking();
        var now = DateTimeOffset.UtcNow;
        var lockExpiration = now.Subtract(GetLockTimeout());
        var throughputSinceUtc = now.AddMinutes(-1);

        var pendingDates = await actions
            .Where(x => x.Status == DurableActionStatus.Pending)
            .Select(x => x.CreatedOnUtc)
            .ToListAsync(cancellationToken);

        var lockedDates = await actions
            .Where(x => x.Status == DurableActionStatus.Locked && x.LockedOnUtc != null)
            .Select(x => x.LockedOnUtc.Value)
            .ToListAsync(cancellationToken);

        var failedDates = await actions
            .Where(x => x.Status == DurableActionStatus.Failed)
            .Select(x => x.CreatedOnUtc)
            .ToListAsync(cancellationToken);

        var enqueueToExecutionLatencies = await actions
            .Where(x => x.StartedOnUtc != null)
            .Select(x => new { x.CreatedOnUtc, x.StartedOnUtc })
            .ToListAsync(cancellationToken);
        var executionLatencies = await actions
            .Where(x => x.StartedOnUtc != null && x.CompletedOnUtc != null)
            .Select(x => new { x.StartedOnUtc, x.CompletedOnUtc })
            .ToListAsync(cancellationToken);
        var terminalLatencies = await actions
            .Where(x => x.TerminalOnUtc != null)
            .Select(x => new { x.CreatedOnUtc, x.TerminalOnUtc })
            .ToListAsync(cancellationToken);
        var completedDates = await actions
            .Where(x => x.Status == DurableActionStatus.Completed && x.CompletedOnUtc != null)
            .Select(x => x.CompletedOnUtc.Value)
            .ToListAsync(cancellationToken);
        var backlogByLane = await actions
            .Where(x => x.Status == DurableActionStatus.Pending || x.Status == DurableActionStatus.Locked)
            .GroupBy(x => x.Lane)
            .Select(x => new { Lane = x.Key, Count = x.Count() })
            .ToListAsync(cancellationToken);
        var backlogByActionKey = await actions
            .Where(x => x.Status == DurableActionStatus.Pending || x.Status == DurableActionStatus.Locked)
            .GroupBy(x => x.Key)
            .Select(x => new { Key = x.Key, Count = x.Count() })
            .ToListAsync(cancellationToken);
        var failuresByActionKey = await actions
            .Where(x => x.Status == DurableActionStatus.Failed)
            .GroupBy(x => x.Key)
            .Select(x => new { Key = x.Key, Count = x.Count() })
            .ToListAsync(cancellationToken);
        var retriesByActionKey = await actions
            .Where(x => x.Attempts > 0)
            .GroupBy(x => x.Key)
            .Select(x => new { Key = x.Key, Count = x.Sum(action => action.Attempts) })
            .ToListAsync(cancellationToken);

        var oldestPendingOnUtc = pendingDates.Count == 0 ? null : (DateTimeOffset?)pendingDates.Min();
        var oldestLockedOnUtc = lockedDates.Count == 0 ? null : (DateTimeOffset?)lockedDates.Min();

        return new MuleDiagnosticsSnapshot
        {
            Pending = await actions.CountAsync(x => x.Status == DurableActionStatus.Pending, cancellationToken),
            Locked = await actions.CountAsync(x => x.Status == DurableActionStatus.Locked, cancellationToken),
            Completed = await actions.CountAsync(x => x.Status == DurableActionStatus.Completed, cancellationToken),
            Failed = await actions.CountAsync(x => x.Status == DurableActionStatus.Failed, cancellationToken),
            LockExpired = lockedDates.Count(x => x <= lockExpiration),
            ThroughputPerMinute = completedDates.Count(x => x >= throughputSinceUtc),
            OldestPendingOnUtc = oldestPendingOnUtc,
            OldestLockedOnUtc = oldestLockedOnUtc,
            OldestFailedOnUtc = failedDates.Count == 0 ? null : failedDates.Min(),
            OldestPendingAge = oldestPendingOnUtc == null ? null : now - oldestPendingOnUtc.Value,
            OldestLockedAge = oldestLockedOnUtc == null ? null : now - oldestLockedOnUtc.Value,
            AverageEnqueueToExecutionLatency = AverageDuration(enqueueToExecutionLatencies.Select(x => x.StartedOnUtc.Value - x.CreatedOnUtc)),
            AverageExecutionLatency = AverageDuration(executionLatencies.Select(x => x.CompletedOnUtc.Value - x.StartedOnUtc.Value)),
            AverageEnqueueToTerminalLatency = AverageDuration(terminalLatencies.Select(x => x.TerminalOnUtc.Value - x.CreatedOnUtc)),
            BacklogByLane = backlogByLane.ToDictionary(x => NormalizeLane(x.Lane), x => x.Count, StringComparer.OrdinalIgnoreCase),
            BacklogByActionKey = backlogByActionKey.ToDictionary(x => x.Key, x => x.Count),
            FailuresByActionKey = failuresByActionKey.ToDictionary(x => x.Key, x => x.Count),
            RetriesByActionKey = retriesByActionKey.ToDictionary(x => x.Key, x => x.Count)
        };
    }

    public void Dispose()
        => _dbContext.Dispose();

    public ValueTask DisposeAsync()
        => _dbContext.DisposeAsync();

    private TimeSpan GetLockTimeout()
        => _settings.LockTimeout <= TimeSpan.Zero ? TimeSpan.FromMinutes(5) : _settings.LockTimeout;

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;

    private static TimeSpan? AverageDuration(IEnumerable<TimeSpan> values)
    {
        var durations = values.ToArray();
        if (durations.Length == 0)
            return null;

        return TimeSpan.FromTicks((long)durations.Average(x => x.Ticks));
    }
}

internal sealed class EntityFrameworkMuleDiagnostics : EntityFrameworkMuleDiagnostics<MuleDbContext>
{
    public EntityFrameworkMuleDiagnostics(IMuleDbContextFactory<MuleDbContext> dbContextFactory, IOptions<MuleSettings> settings)
        : base(dbContextFactory, settings)
    {
    }
}
