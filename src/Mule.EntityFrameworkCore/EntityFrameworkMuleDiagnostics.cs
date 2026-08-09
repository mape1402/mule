namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;
using Mule.Diagnostics;

internal class EntityFrameworkMuleDiagnostics<TDbContext> : IMuleDiagnostics, IDisposable, IAsyncDisposable
    where TDbContext : DbContext
{
    private readonly TDbContext _dbContext;

    public EntityFrameworkMuleDiagnostics(IMuleDbContextFactory<TDbContext> dbContextFactory)
    {
        if (dbContextFactory == null)
            throw new ArgumentNullException(nameof(dbContextFactory));

        _dbContext = dbContextFactory.CreateDbContext();
    }

    public async Task<MuleDiagnosticsSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default)
    {
        var actions = _dbContext.Set<DurableAction>().AsNoTracking();

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

    public void Dispose()
        => _dbContext.Dispose();

    public ValueTask DisposeAsync()
        => _dbContext.DisposeAsync();
}

internal sealed class EntityFrameworkMuleDiagnostics : EntityFrameworkMuleDiagnostics<MuleDbContext>
{
    public EntityFrameworkMuleDiagnostics(IMuleDbContextFactory<MuleDbContext> dbContextFactory)
        : base(dbContextFactory)
    {
    }
}
