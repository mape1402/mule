namespace Mule.FastLane.Redis;

using Microsoft.Extensions.Options;
using StackExchange.Redis;

internal sealed class RedisFastLaneConnection : IAsyncDisposable, IDisposable
{
    private readonly Lazy<ConnectionMultiplexer> _connection;
    private readonly FastLaneRedisOptions _options;

    public RedisFastLaneConnection(IOptions<FastLaneRedisOptions> options)
    {
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _connection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(_options.ConnectionString));
    }

    public IDatabase Database => _connection.Value.GetDatabase(_options.Database);

    public string Prefix => string.IsNullOrWhiteSpace(_options.KeyPrefix) ? "mule" : _options.KeyPrefix.TrimEnd(':');

    public void Dispose()
    {
        if (_connection.IsValueCreated)
            _connection.Value.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection.IsValueCreated)
            await _connection.Value.DisposeAsync();
    }
}
