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
        var ids = await EnqueueManyAsync(
            [MuleIntent.For(key, payload, configure)],
            cancellationToken);

        return ids.Single();
    }

    public async ValueTask<IReadOnlyCollection<Guid>> EnqueueManyAsync(
        IEnumerable<MuleIntent> intents,
        CancellationToken cancellationToken = default)
    {
        if (intents == null)
            throw new ArgumentNullException(nameof(intents));

        var actions = intents
            .Select(CreateAction)
            .ToArray();

        if (actions.Length == 0)
            return Array.Empty<Guid>();

        await _storage.AddRangeAsync(actions, cancellationToken);
        await _storage.SaveChangesAsync(cancellationToken);

        var actionIds = new List<Guid>(actions.Length);
        foreach (var action in actions)
        {
            var actionId = action.Id;
            if (!string.IsNullOrWhiteSpace(action.DeduplicationKey))
                actionId = await _storage.FindByDeduplicationKeyAsync(action.Key, action.DeduplicationKey, cancellationToken)
                    ?? action.Id;

            actionIds.Add(actionId);
            await _commitNotifier.NotifySavedAsync(actionId, action.Lane, cancellationToken);
        }

        return actionIds;
    }

    private DurableAction CreateAction(MuleIntent intent)
    {
        if (intent == null)
            throw new ArgumentException("Intent collection cannot contain null values.", nameof(intent));

        if (string.IsNullOrWhiteSpace(intent.Key.Value))
            throw new ArgumentException("Action key must be provided.", nameof(intent));

        if (intent.Payload == null)
            throw new ArgumentException("Intent payload must be provided.", nameof(intent));

        var options = new EnqueueOptions();
        intent.Configure?.Invoke(options);

        return new DurableAction
        {
            Id = Guid.NewGuid(),
            Key = intent.Key,
            Lane = string.IsNullOrWhiteSpace(options.Lane) ? MuleSettings.DefaultLane : options.Lane,
            Payload = _serializer.Serialize(intent.Payload),
            PayloadType = intent.Payload.GetType().AssemblyQualifiedName,
            Metadata = options.Metadata.Count == 0 ? null : JsonSerializer.Serialize(options.Metadata),
            CorrelationId = options.CorrelationId,
            DeduplicationKey = options.DeduplicationKey,
            Status = DurableActionStatus.Pending,
            CreatedOnUtc = DateTimeOffset.UtcNow
        };
    }
}
