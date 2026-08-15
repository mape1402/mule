namespace Mule.Dispatching;

using System.Threading.Channels;
using Microsoft.Extensions.Options;

internal sealed class ChannelMuleDispatchQueue : IMuleDispatchQueue
{
    private readonly Channel<Guid> _channel;

    public ChannelMuleDispatchQueue(IOptions<MuleSettings> settings)
    {
        var capacity = settings?.Value?.DispatchQueueCapacity ?? 0;

        _channel = capacity > 0
            ? Channel.CreateBounded<Guid>(new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false
            })
            : Channel.CreateUnbounded<Guid>(new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false
            });
    }

    public ValueTask EnqueueAsync(Guid actionId, CancellationToken cancellationToken = default)
        => _channel.Writer.WriteAsync(actionId, cancellationToken);

    public ValueTask<Guid> DequeueAsync(CancellationToken cancellationToken = default)
        => _channel.Reader.ReadAsync(cancellationToken);
}
