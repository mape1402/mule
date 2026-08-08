namespace Mule.Configuration;

using System.Reflection;
using Mule.Dispatching;

internal sealed class MuleRegistrationBuilder : IMuleRegistrationBuilder
{
    private readonly MuleActionRegistry _registry;

    public MuleRegistrationBuilder(MuleActionRegistry registry)
    {
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
    }

    public IMuleRegistrationBuilder AddActionsFromAssembly(Assembly assembly)
    {
        if (assembly == null)
            throw new ArgumentNullException(nameof(assembly));

        foreach (var type in assembly.GetTypes().Where(IsConcreteActionType))
            AddDiscoveredAction(type);

        return this;
    }

    public IMuleRegistrationBuilder AddActionsFromAssemblyContaining<TMarker>()
        => AddActionsFromAssembly(typeof(TMarker).Assembly);

    public IMuleRegistrationBuilder For<TAction, TPayload>(ActionKey key)
        where TAction : class, IMuleAction<TPayload>
    {
        _registry.Add(new ActivatorMuleActionHandler<TAction, TPayload>(key));
        return this;
    }

    public IMuleRegistrationBuilder For<TService, TPayload>(
        ActionKey key,
        Func<TService, MuleActionContext<TPayload>, CancellationToken, ValueTask> execute)
        where TService : notnull
    {
        _registry.Add(new MuleActionHandler<TService, TPayload>(key, execute));
        return this;
    }

    public IMuleRegistrationBuilder For<TService, TPayload>(
        ActionKey key,
        Func<TService, MuleActionContext<TPayload>, CancellationToken, Task> execute)
        where TService : notnull
    {
        if (execute == null)
            throw new ArgumentNullException(nameof(execute));

        Func<TService, MuleActionContext<TPayload>, CancellationToken, ValueTask> adapter =
            async (service, context, cancellationToken) =>
                await execute(service, context, cancellationToken);

        return For(key, adapter);
    }

    private void AddDiscoveredAction(Type actionType)
    {
        var attribute = actionType.GetCustomAttribute<MuleActionAttribute>();

        if (attribute == null)
            return;

        var actionContracts = actionType
            .GetInterfaces()
            .Where(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(IMuleAction<>))
            .ToArray();

        if (actionContracts.Length != 1)
            throw new InvalidOperationException($"Mule action type '{actionType.FullName}' must implement exactly one IMuleAction<TPayload> interface.");

        var payloadType = actionContracts[0].GetGenericArguments()[0];
        _registry.Add(new ActivatorMuleActionHandler(attribute.Key, actionType, payloadType));
    }

    private static bool IsConcreteActionType(Type type)
        => type is { IsClass: true, IsAbstract: false };
}
