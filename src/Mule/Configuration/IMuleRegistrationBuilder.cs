namespace Mule.Configuration;

using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

public interface IMuleRegistrationBuilder
{
    IMuleRegistrationBuilder Configure(Action<MuleSettings> configure);

    IMuleRegistrationBuilder AddServices(Action<IServiceCollection> configure);

    IMuleRegistrationBuilder UseRecovery(MuleRecoveryMode mode);

    IMuleRegistrationBuilder UseCleanup(MuleCleanupMode mode);

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
