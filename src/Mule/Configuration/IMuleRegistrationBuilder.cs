namespace Mule.Configuration;

using System.Reflection;

public interface IMuleRegistrationBuilder
{
    IMuleRegistrationBuilder AddActionsFromAssembly(Assembly assembly);

    IMuleRegistrationBuilder AddActionsFromAssemblyContaining<TMarker>();

    IMuleRegistrationBuilder For<TAction, TPayload>(ActionKey key)
        where TAction : class, IMuleAction<TPayload>;

    IMuleRegistrationBuilder For<TService, TPayload>(
        ActionKey key,
        Func<TService, MuleActionContext<TPayload>, CancellationToken, ValueTask> execute)
        where TService : notnull;

    IMuleRegistrationBuilder For<TService, TPayload>(
        ActionKey key,
        Func<TService, MuleActionContext<TPayload>, CancellationToken, Task> execute)
        where TService : notnull;
}
