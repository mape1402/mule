namespace Mule;

public interface IMuleCancellationService
{
    ValueTask<MuleCancellationResult> CancelAsync(
        Ulid actionId,
        CancellationToken cancellationToken = default);
}
