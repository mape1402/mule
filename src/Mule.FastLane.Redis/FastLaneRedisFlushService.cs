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
    }

    private async Task FlushIntentsAsync(CancellationToken cancellationToken)
    {
        var intents = await _buffer.TakeIntentFlushBatchAsync(GetIntentFlushSize());
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
            await _buffer.CompleteIntentFlushAsync(applied);

            var rejected = results.Where(x => x.Outcome != MuleActionTransitionOutcome.Applied).ToArray();
            if (rejected.Length > 0)
            {
                await _buffer.CancelIntentFlushAsync(
                    intents.Where(x => rejected.Any(r => r.ActionId == x.Action.Id)).ToArray());
                throw new InvalidOperationException(
                    $"FastLane durable projection rejected {rejected.Length} action state(s).");
            }
        }
        catch
        {
            await _buffer.CancelIntentFlushAsync(intents);
            throw;
        }
    }


    private int GetIntentFlushSize()
        => Math.Max(1, _options.IntentFlushSize);

    private TimeSpan GetFlushInterval()
        => _options.FlushInterval <= TimeSpan.Zero ? TimeSpan.FromMilliseconds(50) : _options.FlushInterval;
}
