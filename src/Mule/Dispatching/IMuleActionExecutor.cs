namespace Mule.Dispatching;

internal interface IMuleActionExecutor
{
    ValueTask WaitForCapacityAsync(string lane, CancellationToken cancellationToken = default);

    ValueTask EnqueueAsync(DurableAction action, CancellationToken cancellationToken = default);

    ValueTask EnqueueRangeAsync(IReadOnlyCollection<DurableAction> actions, CancellationToken cancellationToken = default);
}
