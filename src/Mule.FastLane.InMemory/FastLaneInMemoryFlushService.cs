namespace Mule.FastLane.InMemory;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

internal sealed class FastLaneInMemoryFlushService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly InMemoryFastLaneBuffer _buffer;
    private readonly FastLaneInMemoryOptions _options;
    private readonly ILogger<FastLaneInMemoryFlushService> _logger;

    public FastLaneInMemoryFlushService(
        IServiceScopeFactory scopeFactory,
        InMemoryFastLaneBuffer buffer,
        IOptions<FastLaneInMemoryOptions> options,
        ILogger<FastLaneInMemoryFlushService> logger)
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
                _logger.LogError(ex, "Mule FastLane in-memory flush failed.");
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
        var intents = _buffer.TakeIntentFlushBatch(GetIntentFlushSize());
        if (intents.Count == 0)
            return;

        try
        {
            using var scope = _scopeFactory.CreateScope();
            var durableStorage = scope.ServiceProvider.GetRequiredService<IMuleDurableStorage>();
            await durableStorage.AddRangeAsync(intents, cancellationToken);
            await durableStorage.SaveChangesAsync(cancellationToken);
            _buffer.CompleteIntentFlush(intents.Select(x => x.Id).ToArray());
        }
        catch
        {
            _buffer.CancelIntentFlush(intents.Select(x => x.Id).ToArray());
            throw;
        }
    }

    private async Task FlushTerminalsAsync(CancellationToken cancellationToken)
    {
        var terminals = _buffer.TakeTerminalFlushBatch(GetCompletionFlushSize());
        if (terminals.Count == 0)
            return;

        try
        {
            using var scope = _scopeFactory.CreateScope();
            var durableStorage = scope.ServiceProvider.GetRequiredService<IMuleDurableStorage>();

            foreach (var action in terminals)
            {
                if (action.Status == DurableActionStatus.Completed)
                {
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
            _buffer.CompleteTerminalFlush(terminals.Select(x => x.Id).ToArray());
        }
        catch
        {
            _buffer.CancelTerminalFlush(terminals.Select(x => x.Id).ToArray());
            throw;
        }
    }

    private int GetIntentFlushSize()
        => Math.Max(1, _options.IntentFlushSize);

    private int GetCompletionFlushSize()
        => Math.Max(1, _options.CompletionFlushSize);

    private TimeSpan GetFlushInterval()
        => _options.FlushInterval <= TimeSpan.Zero ? TimeSpan.FromMilliseconds(50) : _options.FlushInterval;
}
