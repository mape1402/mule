namespace Mule;

using System.Text.Json;
using Mule.Dispatching;

internal sealed class MuleClient : IMuleClient
{
    private readonly IMuleStorage _storage;
    private readonly IMuleSerializer _serializer;
    private readonly IMuleCommitNotifier _commitNotifier;

    public MuleClient(IMuleStorage storage, IMuleSerializer serializer, IMuleCommitNotifier commitNotifier)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _commitNotifier = commitNotifier ?? throw new ArgumentNullException(nameof(commitNotifier));
    }

    public ValueTask<Guid> EnqueueAsync<TPayload>(
        ActionKey key,
        TPayload payload,
        CancellationToken cancellationToken = default)
        => EnqueueAsync(key, payload, null, cancellationToken);

    public async ValueTask<Guid> EnqueueAsync<TPayload>(
        ActionKey key,
        TPayload payload,
        Action<EnqueueOptions> configure,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(key.Value))
            throw new ArgumentException("Action key must be provided.", nameof(key));

        if (payload == null)
            throw new ArgumentNullException(nameof(payload));

        var options = new EnqueueOptions();
        configure?.Invoke(options);

        var action = new DurableAction
        {
            Id = Guid.NewGuid(),
            Key = key,
            Lane = string.IsNullOrWhiteSpace(options.Lane) ? MuleSettings.DefaultLane : options.Lane,
            Payload = _serializer.Serialize(payload),
            PayloadType = payload.GetType().AssemblyQualifiedName,
            Metadata = options.Metadata.Count == 0 ? null : JsonSerializer.Serialize(options.Metadata),
            CorrelationId = options.CorrelationId,
            DeduplicationKey = options.DeduplicationKey,
            Status = DurableActionStatus.Pending,
            CreatedOnUtc = DateTimeOffset.UtcNow
        };

        await _storage.AddAsync(action, cancellationToken);
        await _storage.SaveChangesAsync(cancellationToken);
        await _commitNotifier.NotifySavedAsync(action.Id, cancellationToken);

        return action.Id;
    }
}
