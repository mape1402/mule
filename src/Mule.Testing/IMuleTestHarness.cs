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
        Guid id,
        DurableActionStatus? status = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default);
}
