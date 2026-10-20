namespace Mule.Testing;

public interface IMuleTestHarness
{
    IReadOnlyCollection<DurableAction> Actions { get; }

    Task<DurableAction> WaitForActionAsync(
        ActionKey key,
        DurableActionStatus? status = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default);

    Task<DurableAction> WaitForActionAsync(
        Ulid id,
        DurableActionStatus? status = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default);

    ValueTask<MuleReplayResult> ReplayAsync(
        Ulid actionId,
        MuleReplayRequest request,
        CancellationToken cancellationToken = default);

    Task<MuleReplayInfo> WaitForReplayAsync(
        Ulid requestId,
        MuleReplayStatus? status = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default);
}
