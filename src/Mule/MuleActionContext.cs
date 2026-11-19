namespace Mule;

using System.Text.Json;

public class MuleActionContext
{
    private MuleActionDeferral _deferral;

    public MuleActionContext(DurableAction action, IServiceProvider services)
    {
        Action = action ?? throw new ArgumentNullException(nameof(action));
        Services = services ?? throw new ArgumentNullException(nameof(services));
    }

    public DurableAction Action { get; }

    public IServiceProvider Services { get; }

    public ActionKey Key => Action.Key;

    public string Lane => Action.Lane;

    public Ulid Id => Action.Id;

    public string CorrelationId => Action.CorrelationId;

    public string DeduplicationKey => Action.DeduplicationKey;

    public IReadOnlyDictionary<string, string> Metadata
        => string.IsNullOrWhiteSpace(Action.Metadata)
            ? new Dictionary<string, string>()
            : JsonSerializer.Deserialize<Dictionary<string, string>>(Action.Metadata) ?? new Dictionary<string, string>();

    public void DeferUntil(DateTimeOffset notBeforeUtc, DeferralReason reason)
    {
        if (notBeforeUtc == default)
            throw new ArgumentException("A valid deferral date is required.", nameof(notBeforeUtc));
        if (string.IsNullOrWhiteSpace(reason.Value))
            throw new ArgumentException("A deferral reason is required.", nameof(reason));

        _deferral = new MuleActionDeferral(notBeforeUtc, reason);
    }

    internal MuleActionDeferral GetDeferral()
        => _deferral;
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

internal sealed record MuleActionDeferral(DateTimeOffset NotBeforeUtc, DeferralReason Reason);
