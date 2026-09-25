namespace Mule.FastLane.Redis;

using Microsoft.Extensions.Options;
using StackExchange.Redis;

internal sealed class RedisFastLaneConnection : IAsyncDisposable, IDisposable
{
    private readonly FastLaneRedisOptions _options;
    private readonly IServiceProvider _serviceProvider;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private IConnectionMultiplexer _connection;
    private bool _disposeConnection;

    public RedisFastLaneConnection(IOptions<FastLaneRedisOptions> options, IServiceProvider serviceProvider)
    {
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    }

    public async ValueTask<IDatabase> GetDatabaseAsync(CancellationToken cancellationToken = default)
        => (await GetConnectionAsync(cancellationToken)).GetDatabase(_options.Database);

    public string Prefix => string.IsNullOrWhiteSpace(_options.KeyPrefix) ? "mule" : _options.KeyPrefix.TrimEnd(':');

    public void Dispose()
    {
        if (_disposeConnection)
            _connection?.Dispose();

        _connectionLock.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposeConnection && _connection != null)
            await _connection.DisposeAsync();

        _connectionLock.Dispose();
    }

    private async ValueTask<IConnectionMultiplexer> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection != null)
            return _connection;

        await _connectionLock.WaitAsync(cancellationToken);
        try
        {
            if (_connection != null)
                return _connection;

            _connection = await CreateConnectionAsync(cancellationToken);
            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private async ValueTask<IConnectionMultiplexer> CreateConnectionAsync(CancellationToken cancellationToken)
    {
        if (_options.ConnectionFactory != null)
        {
            var connection = await _options.ConnectionFactory(_serviceProvider, cancellationToken);
            _disposeConnection = _options.DisposeConnection ?? false;
            return connection ?? throw new InvalidOperationException("The Mule Redis connection factory returned null.");
        }

        _disposeConnection = _options.DisposeConnection ?? true;

        if (_options.ConfigurationOptions != null)
            return await ConnectionMultiplexer.ConnectAsync(_options.ConfigurationOptions);

        var connectionString = string.IsNullOrWhiteSpace(_options.ConnectionString)
            ? "localhost:6379"
            : _options.ConnectionString;
        return await ConnectionMultiplexer.ConnectAsync(connectionString);
    }
}
