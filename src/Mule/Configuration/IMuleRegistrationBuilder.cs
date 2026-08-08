namespace Mule.Configuration;

public interface IMuleRegistrationBuilder
{
    IMuleRegistrationBuilder For<TService, TPayload>(
        ActionKey key,
        Func<TService, MuleActionContext<TPayload>, CancellationToken, ValueTask> execute)
        where TService : notnull;

    IMuleRegistrationBuilder For<TService, TPayload>(
        ActionKey key,
        Func<TService, MuleActionContext<TPayload>, CancellationToken, Task> execute)
        where TService : notnull;
}
