namespace Mule.Testing;

using Mule.InMemory;

internal sealed class MuleTestHarness : IMuleTestHarness
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan PollDelay = TimeSpan.FromMilliseconds(25);
    private readonly IInMemoryMule _mule;
    private readonly IMuleReplayService _replayService;

    public MuleTestHarness(IInMemoryMule mule, IMuleReplayService replayService)
    {
        _mule = mule ?? throw new ArgumentNullException(nameof(mule));
        _replayService = replayService ?? throw new ArgumentNullException(nameof(replayService));
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

    public ValueTask<MuleReplayResult> ReplayAsync(
        Ulid actionId,
        MuleReplayRequest request,
        CancellationToken cancellationToken = default)
        => _replayService.ReplayAsync(actionId, request, cancellationToken);

    public async Task<MuleReplayInfo> WaitForReplayAsync(
        Ulid requestId,
        MuleReplayStatus? status = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        using var timeoutSource = new CancellationTokenSource(timeout ?? DefaultTimeout);
        using var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutSource.Token);

        while (!linkedSource.IsCancellationRequested)
        {
            var replay = await _replayService.GetReplayAsync(requestId, linkedSource.Token);
            if (replay != null && (status == null || replay.Status == status))
                return replay;

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
        throw new TimeoutException($"Mule replay with request id '{requestId}'{statusDescription} was not observed before the timeout.");
    }

    public Task<DurableAction> WaitForActionAsync(
        Ulid id,
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
