namespace Mule;

public interface IMuleReplayService
{
    ValueTask<MuleReplayResult> ReplayAsync(
        Ulid actionId,
        MuleReplayRequest request,
        CancellationToken cancellationToken = default);

    ValueTask<MuleReplayInfo> GetReplayAsync(
        Ulid requestId,
        CancellationToken cancellationToken = default);
}
