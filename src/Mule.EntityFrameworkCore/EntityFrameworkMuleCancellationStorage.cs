namespace Mule.EntityFrameworkCore;

using Microsoft.EntityFrameworkCore;

internal partial class EntityFrameworkMuleStorage<TDbContext>
    where TDbContext : DbContext
{
    public async Task<bool> MarkCancelledAsync(
        Ulid actionId,
        DateTimeOffset cancelledOnUtc,
        CancellationToken cancellationToken = default)
    {
        var action = await Actions.SingleOrDefaultAsync(x => x.Id == actionId, cancellationToken);
        if (action == null)
            return false;
        if (action.Status == DurableActionStatus.Cancelled)
            return true;
        if (action.Status is not (DurableActionStatus.Pending or DurableActionStatus.Locked))
            return false;

        action.Status = DurableActionStatus.Cancelled;
        action.CancelledOnUtc = cancelledOnUtc;
        action.TerminalOnUtc = cancelledOnUtc;
        action.NextAttemptOnUtc = null;
        action.LockedOnUtc = null;
        action.ConcurrencyToken = Ulid.NewUlid();

        try
        {
            await _dbContext.SaveChangesAsync(cancellationToken);
            return true;
        }
        catch (DbUpdateConcurrencyException)
        {
            _dbContext.ChangeTracker.Clear();
            return await Actions
                .AsNoTracking()
                .AnyAsync(
                    x => x.Id == actionId && x.Status == DurableActionStatus.Cancelled,
                    cancellationToken);
        }
    }

    public async Task<MuleCancellationResult> CancelAsync(
        Ulid actionId,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        var action = await Actions.SingleOrDefaultAsync(x => x.Id == actionId, cancellationToken);
        if (action != null)
            return await CancelActionAsync(action, now, cancellationToken);

        var replay = await Replays.SingleOrDefaultAsync(x => x.ReplayActionId == actionId, cancellationToken);
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
        return await SaveCancellationAsync(actionId, now, cancellationToken);
    }

    private async Task<MuleCancellationResult> CancelActionAsync(
        DurableAction action,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        if (action.Status == DurableActionStatus.Cancelled)
            return Result(action.Id, MuleCancellationOutcome.AlreadyCancelled, action.CancelledOnUtc);
        if (action.Status != DurableActionStatus.Pending)
            return Result(action.Id, MuleCancellationOutcome.TooLate);

        action.Status = DurableActionStatus.Cancelled;
        action.CancelledOnUtc = now;
        action.TerminalOnUtc = now;
        action.NextAttemptOnUtc = null;
        action.LockedOnUtc = null;
        action.ConcurrencyToken = Ulid.NewUlid();
        return await SaveCancellationAsync(action.Id, now, cancellationToken);
    }

    private async Task<MuleCancellationResult> SaveCancellationAsync(
        Ulid actionId,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        try
        {
            await _dbContext.SaveChangesAsync(cancellationToken);
            return Result(actionId, MuleCancellationOutcome.Cancelled, now);
        }
        catch (DbUpdateConcurrencyException)
        {
            _dbContext.ChangeTracker.Clear();
            return await ReadCancellationResultAsync(actionId, cancellationToken);
        }
    }

    private async Task<MuleCancellationResult> ReadCancellationResultAsync(
        Ulid actionId,
        CancellationToken cancellationToken)
    {
        var action = await Actions
            .AsNoTracking()
            .SingleOrDefaultAsync(x => x.Id == actionId, cancellationToken);
        if (action != null)
        {
            return action.Status == DurableActionStatus.Cancelled
                ? Result(actionId, MuleCancellationOutcome.AlreadyCancelled, action.CancelledOnUtc)
                : Result(actionId, MuleCancellationOutcome.TooLate);
        }

        var replay = await Replays
            .AsNoTracking()
            .SingleOrDefaultAsync(x => x.ReplayActionId == actionId, cancellationToken);
        if (replay == null)
            return Result(actionId, MuleCancellationOutcome.NotFound);

        return replay.Status == MuleReplayStatus.Cancelled
            ? Result(actionId, MuleCancellationOutcome.AlreadyCancelled, replay.CancelledOnUtc)
            : Result(actionId, MuleCancellationOutcome.TooLate);
    }

    private static MuleCancellationResult Result(
        Ulid actionId,
        MuleCancellationOutcome outcome,
        DateTimeOffset? cancelledOnUtc = null)
        => new(actionId, outcome, cancelledOnUtc);
}
