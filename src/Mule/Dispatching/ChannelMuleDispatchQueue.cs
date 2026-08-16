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
    private int _fairScheduleCursor;

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
        => await DequeueFromLanesAsync(
            () => GetFairLaneNames(),
            cancellationToken);

    public async ValueTask<MuleDispatchItem> DequeueAsync(string lane, CancellationToken cancellationToken = default)
    {
        lane = NormalizeLane(lane);
        var channel = GetChannel(lane);

        while (!cancellationToken.IsCancellationRequested)
        {
            if (!await channel.Reader.WaitToReadAsync(cancellationToken))
                continue;

            if (channel.Reader.TryRead(out var actionId))
                return new MuleDispatchItem(actionId, lane);
        }

        throw new OperationCanceledException(cancellationToken);
    }

    public async ValueTask<MuleDispatchItem> DequeueUnassignedAsync(
        IReadOnlyCollection<string> assignedLanes,
        CancellationToken cancellationToken = default)
        => await DequeueFromLanesAsync(
            () =>
            {
                var assigned = new HashSet<string>(
                    assignedLanes.Select(NormalizeLane),
                    StringComparer.OrdinalIgnoreCase);

                return GetFairLaneNames()
                    .Where(lane => !assigned.Contains(lane))
                    .ToArray();
            },
            cancellationToken);

    private async ValueTask<MuleDispatchItem> DequeueFromLanesAsync(
        Func<IReadOnlyCollection<string>> getLanes,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (var lane in getLanes())
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

    private IReadOnlyCollection<string> GetFairLaneNames()
    {
        lock (_gate)
        {
            var lanes = new[] { MuleSettings.DefaultLane }
                .Concat(_settings.Lanes.Keys)
                .Concat(_channels.Keys)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderByDescending(GetPriority)
                .ThenBy(x => x, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            var schedule = lanes
                .SelectMany(lane => Enumerable.Repeat(lane, GetWeight(lane)))
                .ToArray();

            if (schedule.Length == 0)
                return lanes;

            var start = Math.Abs(_fairScheduleCursor++ % schedule.Length);
            return schedule
                .Skip(start)
                .Concat(schedule.Take(start))
                .Distinct(StringComparer.OrdinalIgnoreCase)
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

    private int GetWeight(string lane)
    {
        var settings = GetLaneSettings(lane);
        var weight = settings?.Weight > 0 ? settings.Weight : Math.Max(1, settings?.Priority ?? 1);
        return Math.Clamp(weight, 1, 100);
    }

    private MuleLaneSettings GetLaneSettings(string lane)
        => _settings.Lanes.TryGetValue(NormalizeLane(lane), out var settings) ? settings : null;

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;
}
