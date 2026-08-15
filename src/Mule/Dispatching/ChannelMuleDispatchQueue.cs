namespace Mule.Dispatching;

using System.Threading.Channels;
using Microsoft.Extensions.Options;

internal sealed class ChannelMuleDispatchQueue : IMuleDispatchQueue
{
    private readonly object _gate = new();
    private readonly Dictionary<string, Channel<Guid>> _channels = new(StringComparer.OrdinalIgnoreCase);
    private readonly Channel<bool> _signals = Channel.CreateUnbounded<bool>(new UnboundedChannelOptions
    {
        SingleReader = false,
        SingleWriter = false
    });
    private readonly MuleSettings _settings;

    public ChannelMuleDispatchQueue(IOptions<MuleSettings> settings)
    {
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
    }

    public async ValueTask EnqueueAsync(Guid actionId, string lane, CancellationToken cancellationToken = default)
    {
        var channel = GetChannel(lane);
        await channel.Writer.WriteAsync(actionId, cancellationToken);
        _signals.Writer.TryWrite(true);
    }

    public async ValueTask<MuleDispatchItem> DequeueAsync(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (var lane in GetLaneNames())
            {
                if (GetChannel(lane).Reader.TryRead(out var actionId))
                    return new MuleDispatchItem(actionId, lane);
            }

            await _signals.Reader.ReadAsync(cancellationToken);
        }

        throw new OperationCanceledException(cancellationToken);
    }

    private Channel<Guid> GetChannel(string lane)
    {
        lane = NormalizeLane(lane);
        lock (_gate)
        {
            if (_channels.TryGetValue(lane, out var channel))
                return channel;

            channel = CreateChannel(lane);
            _channels[lane] = channel;
            return channel;
        }
    }

    private Channel<Guid> CreateChannel(string lane)
    {
        var capacity = GetQueueCapacity(lane);
        if (capacity > 0)
            return Channel.CreateBounded<Guid>(new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false
            });

        return Channel.CreateUnbounded<Guid>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = false
        });
    }

    private IReadOnlyCollection<string> GetLaneNames()
    {
        lock (_gate)
        {
            return new[] { MuleSettings.DefaultLane }
                .Concat(_settings.Lanes.Keys)
                .Concat(_channels.Keys)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderByDescending(GetPriority)
                .ThenBy(x => x, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
    }

    private int GetQueueCapacity(string lane)
    {
        var laneValue = GetLaneSettings(lane)?.DispatchQueueCapacity;
        return Math.Max(0, laneValue > 0 ? laneValue.Value : _settings.DispatchQueueCapacity);
    }

    private int GetPriority(string lane)
        => GetLaneSettings(lane)?.Priority ?? 0;

    private MuleLaneSettings GetLaneSettings(string lane)
        => _settings.Lanes.TryGetValue(NormalizeLane(lane), out var settings) ? settings : null;

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;
}
