namespace Mule;

public interface IMuleTerminalCancellationStorage
{
    Task<bool> MarkCancelledAsync(
        Ulid actionId,
        DateTimeOffset cancelledOnUtc,
        CancellationToken cancellationToken = default);
}
