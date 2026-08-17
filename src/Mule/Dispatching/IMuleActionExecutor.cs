namespace Mule.Dispatching;

internal interface IMuleActionExecutor
{
    ValueTask WaitForCapacityAsync(string lane, CancellationToken cancellationToken = default);

    ValueTask EnqueueAsync(DurableAction action, CancellationToken cancellationToken = default);
}
