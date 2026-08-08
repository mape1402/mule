namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Mule.Diagnostics;

internal sealed class EntityFrameworkMuleDiagnostics : IMuleDiagnostics
{
    private readonly MuleDbContext _dbContext;

    public EntityFrameworkMuleDiagnostics(MuleDbContext dbContext)
    {
        _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
    }

    public async Task<MuleDiagnosticsSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default)
    {
        var actions = _dbContext.Actions.AsNoTracking();

        var pendingDates = await actions
            .Where(x => x.Status == DurableActionStatus.Pending)
            .Select(x => x.CreatedOnUtc)
            .ToListAsync(cancellationToken);

        var failedDates = await actions
            .Where(x => x.Status == DurableActionStatus.Failed)
            .Select(x => x.CreatedOnUtc)
            .ToListAsync(cancellationToken);

        return new MuleDiagnosticsSnapshot
        {
            Pending = await actions.CountAsync(x => x.Status == DurableActionStatus.Pending, cancellationToken),
            Locked = await actions.CountAsync(x => x.Status == DurableActionStatus.Locked, cancellationToken),
            Completed = await actions.CountAsync(x => x.Status == DurableActionStatus.Completed, cancellationToken),
            Failed = await actions.CountAsync(x => x.Status == DurableActionStatus.Failed, cancellationToken),
            OldestPendingOnUtc = pendingDates.Count == 0 ? null : pendingDates.Min(),
            OldestFailedOnUtc = failedDates.Count == 0 ? null : failedDates.Min()
        };
    }
}
