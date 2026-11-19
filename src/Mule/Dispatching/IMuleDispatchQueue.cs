namespace Mule.Dispatching;

public interface IMuleDispatchQueue
{
    bool TryEnqueue(Ulid actionId, string lane)
        => false;

    ValueTask EnqueueAsync(Ulid actionId, string lane, CancellationToken cancellationToken = default);

    ValueTask<MuleDispatchItem> DequeueAsync(CancellationToken cancellationToken = default);

    ValueTask<MuleDispatchItem> DequeueAsync(string lane, CancellationToken cancellationToken = default);

    ValueTask<MuleDispatchItem> DequeueUnassignedAsync(
        IReadOnlyCollection<string> assignedLanes,
        CancellationToken cancellationToken = default);
}
