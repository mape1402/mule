namespace Mule;

public sealed record MuleCancellationResult(
    Ulid ActionId,
    MuleCancellationOutcome Outcome,
    DateTimeOffset? CancelledOnUtc = null)
{
    public bool IsCancelled => Outcome is MuleCancellationOutcome.Cancelled or MuleCancellationOutcome.AlreadyCancelled;
}
