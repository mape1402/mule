namespace Mule.Configuration;

using Mule.Dispatching;

internal sealed class MuleRegistrationBuilder : IMuleRegistrationBuilder
{
    private readonly MuleActionRegistry _registry;

    public MuleRegistrationBuilder(MuleActionRegistry registry)
    {
        _registry = registry ?? throw new ArgumentNullException(nameof(registry));
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
}
