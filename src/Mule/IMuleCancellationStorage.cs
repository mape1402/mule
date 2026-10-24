namespace Mule;

public interface IMuleCancellationStorage
{
    Task<MuleCancellationResult> CancelAsync(
        Ulid actionId,
        DateTimeOffset now,
        CancellationToken cancellationToken = default);
}
