namespace Mule.Dispatching;

internal interface IMuleActionHandler
{
    ActionKey Key { get; }

    ValueTask ExecuteAsync(DurableAction action, IServiceProvider services, CancellationToken cancellationToken);
}
