namespace Mule.FastLane.Redis.Tests;

using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule.EntityFrameworkCore;
using StackExchange.Redis;

[Collection("FastLane Redis")]
public sealed class RedisFastLaneTests
{
    public const string ConnectionStringEnvironmentVariable = "MULE_REDIS_CONNECTION_STRING";

    private static readonly ActionKey Key = ActionKey.From("tests.fastlane.redis.capture.v1");

    [RedisFact]
    public async Task EnqueueManyAsync_Should_Buffer_In_Redis_And_Flush_Durable_State_In_Order()
    {
        var keyPrefix = $"mule-tests:{Guid.NewGuid():N}";
        using var host = CreateHost(out var databasePath, keyPrefix);

        try
        {
            await EnsureDatabaseAsync(host);
            await host.StartAsync();

            using (var scope = host.Services.CreateScope())
            {
                var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
                await client.EnqueueManyAsync(Enumerable.Range(0, 25)
                    .Select(index => MuleIntent.For(Key, new TestPayload($"redis-{index}"))));
            }

            var probe = host.Services.GetRequiredService<TestProbe>();
            await probe.WaitForCountAsync(25);
            await WaitForDurableCountAsync(host, DurableActionStatus.Completed, 25);
            await host.StopAsync();

            Assert.Equal(25, probe.Values.Count);
        }
        finally
        {
            await CleanupRedisAsync(keyPrefix);
            TryDelete(databasePath);
        }
    }

    private static IHost CreateHost(out string databasePath, string keyPrefix)
    {
        databasePath = Path.Combine(Path.GetTempPath(), $"mule-fastlane-redis-{Guid.NewGuid():N}.db");
        var capturedPath = databasePath;

        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.Configure<MuleSettings>(settings =>
                {
                    settings.DispatchInterval = TimeSpan.FromMilliseconds(25);
                    settings.RecoveryMode = MuleRecoveryMode.Scheduled;
                    settings.RetryDelay = TimeSpan.FromMilliseconds(25);
                    settings.MaxAttempts = 1;
                    settings.CleanupMode = MuleCleanupMode.Disabled;
                });

                services.AddSingleton<TestProbe>();
                services.AddDbContext<MuleDbContext>(options => options.UseSqlite($"Data Source={capturedPath}"));
                services.AddMule(mule => mule
                    .UseEntityFrameworkCore<MuleDbContext>()
                    .UseFastLaneRedis(options =>
                    {
                        options.ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);
                        options.KeyPrefix = keyPrefix;
                        options.IntentFlushSize = 100;
                        options.CompletionFlushSize = 100;
                        options.FlushInterval = TimeSpan.FromMilliseconds(10);
                    })
                    .AddActionsFromAssemblyContaining<RedisFastLaneTests>());
            })
            .Build();
    }

    private static async Task EnsureDatabaseAsync(IHost host)
    {
        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
        await db.Database.EnsureCreatedAsync();
    }

    private static async Task WaitForDurableCountAsync(IHost host, DurableActionStatus status, int count)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!timeout.IsCancellationRequested)
        {
            using var scope = host.Services.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
            if (await db.Actions.AsNoTracking().CountAsync(x => x.Status == status, timeout.Token) >= count)
                return;

            await Task.Delay(25, timeout.Token);
        }
    }

    private static async Task CleanupRedisAsync(string keyPrefix)
    {
        var connectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
            return;

        await using var connection = await ConnectionMultiplexer.ConnectAsync(connectionString);
        var database = connection.GetDatabase();
        foreach (var endpoint in connection.GetEndPoints())
        {
            var server = connection.GetServer(endpoint);
            await foreach (var key in server.KeysAsync(pattern: $"{keyPrefix}:*"))
                await database.KeyDeleteAsync(key);
        }
    }

    private static void TryDelete(string databasePath)
    {
        try
        {
            if (File.Exists(databasePath))
                File.Delete(databasePath);
        }
        catch
        {
        }
    }

    private sealed record TestPayload(string Value);

    [MuleAction("tests.fastlane.redis.capture.v1")]
    private sealed class TestAction : IMuleAction<TestPayload>
    {
        private readonly TestProbe _probe;

        public TestAction(TestProbe probe)
        {
            _probe = probe;
        }

        public ValueTask ExecuteAsync(MuleActionContext<TestPayload> context, CancellationToken cancellationToken)
        {
            _probe.Values.Add(context.Payload.Value);
            _probe.Signal();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestProbe
    {
        private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ConcurrentBag<string> Values { get; } = new();

        public void Signal()
            => _completion.TrySetResult();

        public async Task WaitForCountAsync(int count)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            while (!timeout.IsCancellationRequested)
            {
                if (Values.Count >= count)
                    return;

                await Task.Delay(25, timeout.Token);
            }
        }
    }
}

[CollectionDefinition("FastLane Redis", DisableParallelization = true)]
public sealed class FastLaneRedisCollection
{
}
