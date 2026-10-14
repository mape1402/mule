namespace Mule;

public interface IMuleClient
{
    ValueTask<Ulid> EnqueueAsync<TPayload>(
        ActionKey key,
        TPayload payload,
        CancellationToken cancellationToken = default);

    ValueTask<Ulid> EnqueueAsync<TPayload>(
        ActionKey key,
        TPayload payload,
        Action<EnqueueOptions> configure,
        CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyCollection<Ulid>> EnqueueManyAsync(
        IEnumerable<MuleIntent> intents,
        CancellationToken cancellationToken = default);
}
