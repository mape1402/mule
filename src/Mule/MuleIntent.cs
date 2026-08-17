namespace Mule;

public sealed class MuleIntent
{
    private MuleIntent(ActionKey key, object payload, Action<EnqueueOptions> configure)
    {
        Key = key;
        Payload = payload;
        Configure = configure;
    }

    public ActionKey Key { get; }

    public object Payload { get; }

    public Action<EnqueueOptions> Configure { get; }

    public static MuleIntent For<TPayload>(
        ActionKey key,
        TPayload payload,
        Action<EnqueueOptions> configure = null)
        => new(key, payload, configure);
}
