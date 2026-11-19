namespace Mule.Dispatching;

internal interface IMuleActionHandler
{
    ActionKey Key { get; }

    ValueTask<MuleActionDeferral> ExecuteAsync(DurableAction action, IServiceProvider services, CancellationToken cancellationToken);
}
