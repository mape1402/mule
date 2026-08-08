namespace Mule.Dispatching;

using Microsoft.Extensions.DependencyInjection;

internal sealed class MuleActionHandler<TService, TPayload> : IMuleActionHandler
    where TService : notnull
{
    private readonly Func<TService, MuleActionContext<TPayload>, CancellationToken, ValueTask> _execute;

    public MuleActionHandler(
        ActionKey key,
        Func<TService, MuleActionContext<TPayload>, CancellationToken, ValueTask> execute)
    {
        if (string.IsNullOrWhiteSpace(key.Value))
            throw new ArgumentException("Action key must be provided.", nameof(key));

        Key = key;
        _execute = execute ?? throw new ArgumentNullException(nameof(execute));
    }

    public ActionKey Key { get; }

    public async ValueTask ExecuteAsync(DurableAction action, IServiceProvider services, CancellationToken cancellationToken)
    {
        var serializer = services.GetRequiredService<IMuleSerializer>();
        var service = services.GetRequiredService<TService>();
        var payloadType = Type.GetType(action.PayloadType, throwOnError: true);
        var payload = (TPayload)serializer.Deserialize(action.Payload, payloadType);
        var context = new MuleActionContext<TPayload>(action, services, payload);

        await _execute(service, context, cancellationToken);
    }
}

internal sealed class ActivatorMuleActionHandler<TAction, TPayload> : IMuleActionHandler
    where TAction : class, IMuleAction<TPayload>
{
    public ActivatorMuleActionHandler(ActionKey key)
    {
        if (string.IsNullOrWhiteSpace(key.Value))
            throw new ArgumentException("Action key must be provided.", nameof(key));

        Key = key;
    }

    public ActionKey Key { get; }

    public async ValueTask ExecuteAsync(DurableAction action, IServiceProvider services, CancellationToken cancellationToken)
    {
        var serializer = services.GetRequiredService<IMuleSerializer>();
        var payloadType = Type.GetType(action.PayloadType, throwOnError: true);
        var payload = (TPayload)serializer.Deserialize(action.Payload, payloadType);
        var context = new MuleActionContext<TPayload>(action, services, payload);
        var handler = ActivatorUtilities.CreateInstance<TAction>(services);

        await handler.ExecuteAsync(context, cancellationToken);
    }
}

internal sealed class ActivatorMuleActionHandler : IMuleActionHandler
{
    private readonly Type _actionType;
    private readonly Type _payloadType;

    public ActivatorMuleActionHandler(ActionKey key, Type actionType, Type payloadType)
    {
        if (string.IsNullOrWhiteSpace(key.Value))
            throw new ArgumentException("Action key must be provided.", nameof(key));

        Key = key;
        _actionType = actionType ?? throw new ArgumentNullException(nameof(actionType));
        _payloadType = payloadType ?? throw new ArgumentNullException(nameof(payloadType));
    }

    public ActionKey Key { get; }

    public async ValueTask ExecuteAsync(DurableAction action, IServiceProvider services, CancellationToken cancellationToken)
    {
        var serializer = services.GetRequiredService<IMuleSerializer>();
        var storedPayloadType = Type.GetType(action.PayloadType, throwOnError: true);

        if (!_payloadType.IsAssignableFrom(storedPayloadType))
            throw new InvalidOperationException($"Mule action '{Key}' expects payload type '{_payloadType.FullName}' but received '{storedPayloadType.FullName}'.");

        var payload = serializer.Deserialize(action.Payload, storedPayloadType);
        var contextType = typeof(MuleActionContext<>).MakeGenericType(_payloadType);
        var context = Activator.CreateInstance(contextType, action, services, payload);
        var handler = ActivatorUtilities.CreateInstance(services, _actionType);
        var method = _actionType.GetMethod(nameof(IMuleAction<object>.ExecuteAsync), new[] { contextType, typeof(CancellationToken) });

        if (method == null)
            throw new InvalidOperationException($"Mule action type '{_actionType.FullName}' does not expose a valid ExecuteAsync method.");

        var result = method.Invoke(handler, new[] { context, cancellationToken });
        await (ValueTask)result;
    }
}
