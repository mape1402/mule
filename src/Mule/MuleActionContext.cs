namespace Mule;

using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

public class MuleActionContext
{
    public MuleActionContext(DurableAction action, IServiceProvider services)
    {
        Action = action ?? throw new ArgumentNullException(nameof(action));
        Services = services ?? throw new ArgumentNullException(nameof(services));
    }

    public DurableAction Action { get; }

    public IServiceProvider Services { get; }

    public ActionKey Key => Action.Key;

    public Guid Id => Action.Id;

    public string CorrelationId => Action.CorrelationId;

    public string DeduplicationKey => Action.DeduplicationKey;

    public IReadOnlyDictionary<string, string> Metadata
        => string.IsNullOrWhiteSpace(Action.Metadata)
            ? new Dictionary<string, string>()
            : JsonSerializer.Deserialize<Dictionary<string, string>>(Action.Metadata) ?? new Dictionary<string, string>();
}

public sealed class MuleActionContext<TPayload> : MuleActionContext
{
    public MuleActionContext(DurableAction action, IServiceProvider services, TPayload payload)
        : base(action, services)
    {
        Payload = payload;
    }

    public TPayload Payload { get; }
}
