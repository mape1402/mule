namespace Mule.Redis;

using StackExchange.Redis;

public sealed class RedisMuleOptions
{
    public string ConnectionString { get; set; } = "localhost:6379";

    public ConfigurationOptions ConfigurationOptions { get; set; }

    public Func<IServiceProvider, CancellationToken, ValueTask<IConnectionMultiplexer>> ConnectionFactory { get; set; }

    public bool? DisposeConnection { get; set; }

    public string KeyPrefix { get; set; } = "mule";

    public int Database { get; set; } = -1;

    public int WriteBatchSize { get; set; } = 500;

    public int MinimumReplicaAcknowledgements { get; set; }

    public TimeSpan ReplicaAcknowledgementTimeout { get; set; } = TimeSpan.FromSeconds(1);

    public RedisAmbientTransactionMode AmbientTransactions { get; set; } = RedisAmbientTransactionMode.Reject;
}
