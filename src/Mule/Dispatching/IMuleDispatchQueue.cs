namespace Mule.Dispatching;

public interface IMuleDispatchQueue
{
    ValueTask EnqueueAsync(Guid actionId, CancellationToken cancellationToken = default);

    ValueTask<Guid> DequeueAsync(CancellationToken cancellationToken = default);
}
