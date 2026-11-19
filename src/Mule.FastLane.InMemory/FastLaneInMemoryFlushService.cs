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
            var projectionStorage = durableStorage as IMuleStateProjectionStorage
                ?? throw new NotSupportedException(
                    $"Durable storage '{durableStorage.GetType().FullName}' does not support FastLane state projection.");
            var results = await projectionStorage.ProjectStateRangeAsync(intents, cancellationToken);
            var appliedIds = results
                .Where(x => x.Outcome == MuleActionTransitionOutcome.Applied)
                .Select(x => x.ActionId)
                .ToHashSet();
            var applied = intents.Where(x => appliedIds.Contains(x.Action.Id)).ToArray();
            _buffer.CompleteIntentFlush(applied);

            var rejected = results.Where(x => x.Outcome != MuleActionTransitionOutcome.Applied).ToArray();
            if (rejected.Length > 0)
            {
                _buffer.CancelIntentFlush(intents.Where(x => rejected.Any(r => r.ActionId == x.Action.Id)).ToArray());
                throw new InvalidOperationException(
                    $"FastLane durable projection rejected {rejected.Length} action state(s).");
            }
        }
        catch
        {
            _buffer.CancelIntentFlush(intents);
            throw;
        }
    }

    private int GetIntentFlushSize()
        => Math.Max(1, _options.IntentFlushSize);

    private TimeSpan GetFlushInterval()
        => _options.FlushInterval <= TimeSpan.Zero ? TimeSpan.FromMilliseconds(50) : _options.FlushInterval;
}
