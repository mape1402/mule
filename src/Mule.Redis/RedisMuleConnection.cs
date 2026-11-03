namespace Mule.Redis;

using Microsoft.Extensions.Options;
using StackExchange.Redis;

internal sealed class RedisMuleConnection : IAsyncDisposable, IDisposable
{
    internal const string SchemaVersion = "1";

    private readonly RedisMuleOptions _options;
    private readonly IServiceProvider _serviceProvider;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly SemaphoreSlim _schemaLock = new(1, 1);
    private IConnectionMultiplexer _connection;
    private bool _disposeConnection;
    private bool _schemaValidated;

    public RedisMuleConnection(IOptions<RedisMuleOptions> options, IServiceProvider serviceProvider)
    {
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        Keyspace = new RedisMuleKeyspace(_options.KeyPrefix);
    }

    public RedisMuleKeyspace Keyspace { get; }

    public int WriteBatchSize => Math.Max(1, _options.WriteBatchSize);

    public RedisAmbientTransactionMode AmbientTransactions => _options.AmbientTransactions;

    public async ValueTask<IDatabase> GetDatabaseAsync(CancellationToken cancellationToken = default)
    {
        var connection = await GetConnectionAsync(cancellationToken);
        var database = connection.GetDatabase(_options.Database);
        await EnsureSchemaAsync(database, cancellationToken);
        return database;
    }

    public async Task AcknowledgeReplicationAsync(IDatabase database, CancellationToken cancellationToken = default)
    {
        var replicas = Math.Max(0, _options.MinimumReplicaAcknowledgements);
        if (replicas == 0)
            return;

        cancellationToken.ThrowIfCancellationRequested();
        var timeout = _options.ReplicaAcknowledgementTimeout <= TimeSpan.Zero
            ? TimeSpan.FromSeconds(1)
            : _options.ReplicaAcknowledgementTimeout;
        var result = await database.ExecuteAsync("WAIT", replicas, Math.Max(1L, (long)timeout.TotalMilliseconds));
        cancellationToken.ThrowIfCancellationRequested();

        if ((long)result < replicas)
            throw new TimeoutException($"Redis acknowledged the Mule write on fewer than {replicas} replica(s).");
    }

    public void Dispose()
    {
        if (_disposeConnection)
            _connection?.Dispose();

        _connectionLock.Dispose();
        _schemaLock.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposeConnection && _connection != null)
            await _connection.DisposeAsync();

        _connectionLock.Dispose();
        _schemaLock.Dispose();
    }

    private async ValueTask<IConnectionMultiplexer> GetConnectionAsync(CancellationToken cancellationToken)
    {
        if (_connection != null)
            return _connection;

        await _connectionLock.WaitAsync(cancellationToken);
        try
        {
            if (_connection == null)
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

    private async Task EnsureSchemaAsync(IDatabase database, CancellationToken cancellationToken)
    {
        if (_schemaValidated)
            return;

        await _schemaLock.WaitAsync(cancellationToken);
        try
        {
            if (_schemaValidated)
                return;

            await database.StringSetAsync(Keyspace.Schema, SchemaVersion, when: When.NotExists);
            var persisted = await database.StringGetAsync(Keyspace.Schema);
            if (!persisted.HasValue || !string.Equals(persisted.ToString(), SchemaVersion, StringComparison.Ordinal))
                throw new InvalidOperationException($"Redis contains an unsupported Mule schema version '{persisted}'.");

            _schemaValidated = true;
        }
        finally
        {
            _schemaLock.Release();
        }
    }
}
