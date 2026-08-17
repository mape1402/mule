namespace Mule;

public interface IMuleClient
{
    ValueTask<Guid> EnqueueAsync<TPayload>(
        ActionKey key,
        TPayload payload,
        CancellationToken cancellationToken = default);

    ValueTask<Guid> EnqueueAsync<TPayload>(
        ActionKey key,
        TPayload payload,
        Action<EnqueueOptions> configure,
        CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyCollection<Guid>> EnqueueManyAsync(
        IEnumerable<MuleIntent> intents,
        CancellationToken cancellationToken = default);
}
