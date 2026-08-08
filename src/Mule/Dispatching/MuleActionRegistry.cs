namespace Mule.Dispatching;

public sealed class MuleActionRegistry : IMuleActionRegistry
{
    private readonly Dictionary<ActionKey, IMuleActionHandler> _handlers = new();

    internal void Add(IMuleActionHandler handler)
    {
        if (handler == null)
            throw new ArgumentNullException(nameof(handler));

        if (_handlers.ContainsKey(handler.Key))
            throw new InvalidOperationException($"A Mule handler is already registered for action key '{handler.Key}'.");

        _handlers.Add(handler.Key, handler);
    }

    public bool TryGet(ActionKey key, out object handler)
    {
        var found = _handlers.TryGetValue(key, out var typedHandler);
        handler = typedHandler;
        return found;
    }

    internal bool TryGetHandler(ActionKey key, out IMuleActionHandler handler)
        => _handlers.TryGetValue(key, out handler);
}
