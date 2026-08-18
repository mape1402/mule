namespace Mule;

public interface IMuleBatchTerminalStorage
{
    Task MarkCompletedRangeAsync(
        IReadOnlyCollection<MuleCompletedAction> actions,
        CancellationToken cancellationToken = default);
}
