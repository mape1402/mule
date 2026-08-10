namespace Mule.Testing;

using Mule.InMemory;

internal sealed class MuleTestHarness : IMuleTestHarness
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan PollDelay = TimeSpan.FromMilliseconds(25);
    private readonly IInMemoryMule _mule;

    public MuleTestHarness(IInMemoryMule mule)
    {
        _mule = mule ?? throw new ArgumentNullException(nameof(mule));
    }

    public IReadOnlyCollection<DurableAction> Actions => _mule.Actions;

    public Task<DurableAction> WaitForActionAsync(
        ActionKey key,
        DurableActionStatus? status = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
        => WaitForActionAsync(
            action => action.Key == key,
            $"key '{key}'",
            status,
            timeout,
            cancellationToken);

    public Task<DurableAction> WaitForActionAsync(
        Guid id,
        DurableActionStatus? status = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
        => WaitForActionAsync(
            action => action.Id == id,
            $"id '{id}'",
            status,
            timeout,
            cancellationToken);

    private async Task<DurableAction> WaitForActionAsync(
        Func<DurableAction, bool> match,
        string description,
        DurableActionStatus? status,
        TimeSpan? timeout,
        CancellationToken cancellationToken)
    {
        using var timeoutSource = new CancellationTokenSource(timeout ?? DefaultTimeout);
        using var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutSource.Token);

        while (!linkedSource.IsCancellationRequested)
        {
            var action = Actions
                .Where(match)
                .OrderByDescending(x => x.CreatedOnUtc)
                .FirstOrDefault();

            if (action != null && (status == null || action.Status == status))
                return action;

            try
            {
                await Task.Delay(PollDelay, linkedSource.Token);
            }
            catch (OperationCanceledException) when (timeoutSource.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
            {
                break;
            }
        }

        var statusDescription = status == null ? null : $" with status '{status}'";
        throw new TimeoutException($"Mule action with {description}{statusDescription} was not observed before the timeout.");
    }
}
