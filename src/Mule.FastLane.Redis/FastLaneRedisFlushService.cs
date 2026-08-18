namespace Mule.FastLane.Redis;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

internal sealed class FastLaneRedisFlushService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly RedisFastLaneBuffer _buffer;
    private readonly FastLaneRedisOptions _options;
    private readonly ILogger<FastLaneRedisFlushService> _logger;

    public FastLaneRedisFlushService(
        IServiceScopeFactory scopeFactory,
        RedisFastLaneBuffer buffer,
        IOptions<FastLaneRedisOptions> options,
        ILogger<FastLaneRedisFlushService> logger)
    {
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        _buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await FlushOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mule FastLane Redis flush failed.");
            }

            try
            {
                await Task.Delay(GetFlushInterval(), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task FlushOnceAsync(CancellationToken cancellationToken)
    {
        await FlushIntentsAsync(cancellationToken);
        await FlushTerminalsAsync(cancellationToken);
    }

    private async Task FlushIntentsAsync(CancellationToken cancellationToken)
    {
        var intents = await _buffer.TakeIntentFlushBatchAsync(GetIntentFlushSize());
        if (intents.Count == 0)
            return;

        using var scope = _scopeFactory.CreateScope();
        var durableStorage = scope.ServiceProvider.GetRequiredService<IMuleDurableStorage>();
        await durableStorage.AddRangeAsync(intents, cancellationToken);
        await durableStorage.SaveChangesAsync(cancellationToken);
        await _buffer.CompleteIntentFlushAsync(intents.Select(x => x.Id).ToArray());
    }

    private async Task FlushTerminalsAsync(CancellationToken cancellationToken)
    {
        var terminals = await _buffer.TakeTerminalFlushBatchAsync(GetCompletionFlushSize());
        if (terminals.Count == 0)
            return;

        using var scope = _scopeFactory.CreateScope();
        var durableStorage = scope.ServiceProvider.GetRequiredService<IMuleDurableStorage>();
        var completed = terminals
            .Where(x => x.Status == DurableActionStatus.Completed)
            .Select(x => new MuleCompletedAction(x.Id, x.CompletedOnUtc ?? DateTimeOffset.UtcNow))
            .ToArray();
        var batchTerminalStorage = durableStorage as IMuleBatchTerminalStorage;

        if (batchTerminalStorage != null && completed.Length > 0)
            await batchTerminalStorage.MarkCompletedRangeAsync(completed, cancellationToken);

        foreach (var action in terminals)
        {
            if (action.Status == DurableActionStatus.Completed)
            {
                if (batchTerminalStorage != null)
                    continue;

                await durableStorage.MarkCompletedAsync(
                    action.Id,
                    action.CompletedOnUtc ?? DateTimeOffset.UtcNow,
                    cancellationToken);
                continue;
            }

            if (action.Status == DurableActionStatus.Failed)
            {
                await durableStorage.MarkFailedAsync(
                    action.Id,
                    action.LastError,
                    action.TerminalOnUtc ?? DateTimeOffset.UtcNow,
                    null,
                    cancellationToken);
            }
        }

        await durableStorage.SaveChangesAsync(cancellationToken);
        await _buffer.CompleteTerminalFlushAsync(terminals.Select(x => x.Id).ToArray());
    }

    private int GetIntentFlushSize()
        => Math.Max(1, _options.IntentFlushSize);

    private int GetCompletionFlushSize()
        => Math.Max(1, _options.CompletionFlushSize);

    private TimeSpan GetFlushInterval()
        => _options.FlushInterval <= TimeSpan.Zero ? TimeSpan.FromMilliseconds(50) : _options.FlushInterval;
}
