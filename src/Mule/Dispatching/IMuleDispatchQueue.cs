namespace Mule.Dispatching;

public interface IMuleDispatchQueue
{
    ValueTask EnqueueAsync(Guid actionId, string lane, CancellationToken cancellationToken = default);

    ValueTask<MuleDispatchItem> DequeueAsync(CancellationToken cancellationToken = default);

    ValueTask<MuleDispatchItem> DequeueAsync(string lane, CancellationToken cancellationToken = default);

    ValueTask<MuleDispatchItem> DequeueUnassignedAsync(
        IReadOnlyCollection<string> assignedLanes,
        CancellationToken cancellationToken = default);
}
