namespace Mule.FastLane.Redis.Tests;

using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

[Collection("FastLane Redis")]
public sealed class RedisFlushRaceTests : IAsyncLifetime
{
    private static readonly ActionKey Key = ActionKey.From("tests.fastlane.redis.flush-race.v1");
    private readonly string _prefix = $"mule-tests:{Guid.NewGuid():N}";

    [RedisFact]
    public async Task Stale_Flush_Acknowledgements_Should_Preserve_Newer_Intent_And_Terminal_State()
    {
        using var services = new ServiceCollection().BuildServiceProvider();
        await using var connection = new RedisFastLaneConnection(
            Options.Create(new FastLaneRedisOptions
            {
                ConnectionString = Environment.GetEnvironmentVariable(RedisFastLaneTests.ConnectionStringEnvironmentVariable),
                KeyPrefix = _prefix
            }),
            services);
        var buffer = new RedisFastLaneBuffer(
            connection,
            Options.Create(new FastLaneRedisOptions { KeyPrefix = _prefix }));
        var now = DateTimeOffset.UtcNow;
        var action = CreateAction(now);

        Assert.True(await buffer.TryAddAsync(action));
        Assert.NotNull(await buffer.LockAsync(action.Id, TimeSpan.FromMinutes(1), now));

        var staleIntent = Assert.Single(await buffer.TakeIntentFlushBatchAsync(1));
        Assert.True(await buffer.MarkCompletedAsync(action.Id, now.AddSeconds(1)));
        await buffer.CompleteIntentFlushAsync([staleIntent]);

        var currentIntent = Assert.Single(await buffer.TakeIntentFlushBatchAsync(1));
        Assert.Equal(staleIntent.Action.ConcurrencyToken, currentIntent.ExpectedConcurrencyToken);
        Assert.NotEqual(staleIntent.Action.ConcurrencyToken, currentIntent.Action.ConcurrencyToken);
        await buffer.CompleteIntentFlushAsync([currentIntent]);

        var database = await connection.GetDatabaseAsync();
        Assert.False(await database.KeyExistsAsync($"{_prefix}:fastlane:action:{action.Id}"));
    }

    [RedisFact]
    public async Task Cancelled_Flush_Claims_Should_Release_Current_Envelope_Without_Replacing_It()
    {
        using var services = new ServiceCollection().BuildServiceProvider();
        await using var connection = new RedisFastLaneConnection(
            Options.Create(new FastLaneRedisOptions
            {
                ConnectionString = Environment.GetEnvironmentVariable(RedisFastLaneTests.ConnectionStringEnvironmentVariable),
                KeyPrefix = _prefix
            }),
            services);
        var buffer = new RedisFastLaneBuffer(
            connection,
            Options.Create(new FastLaneRedisOptions { KeyPrefix = _prefix }));
        var now = DateTimeOffset.UtcNow;
        var action = CreateAction(now);

        Assert.True(await buffer.TryAddAsync(action));
        Assert.NotNull(await buffer.LockAsync(action.Id, TimeSpan.FromMinutes(1), now));
        var intent = Assert.Single(await buffer.TakeIntentFlushBatchAsync(1));
        Assert.True(await buffer.MarkCompletedAsync(action.Id, now.AddSeconds(1)));
        await buffer.CancelIntentFlushAsync([intent]);

        var database = await connection.GetDatabaseAsync();
        var value = await database.StringGetAsync($"{_prefix}:fastlane:action:{action.Id}");
        using (var document = JsonDocument.Parse((string)value))
        {
            Assert.Equal((int)DurableActionStatus.Completed, document.RootElement.GetProperty("status").GetInt32());
            Assert.True(document.RootElement.GetProperty("terminalDirty").GetBoolean());
            Assert.False(document.RootElement.GetProperty("intentFlushing").GetBoolean());
        }

        var current = Assert.Single(await buffer.TakeIntentFlushBatchAsync(1));
        Assert.Equal(DurableActionStatus.Completed, current.Action.Status);
        await buffer.CompleteIntentFlushAsync([current]);
        Assert.False(await database.KeyExistsAsync($"{_prefix}:fastlane:action:{action.Id}"));
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        var connectionString = Environment.GetEnvironmentVariable(RedisFastLaneTests.ConnectionStringEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
            return;

        await using var connection = await ConnectionMultiplexer.ConnectAsync(connectionString);
        var database = connection.GetDatabase();
        foreach (var endpoint in connection.GetEndPoints())
        {
            var server = connection.GetServer(endpoint);
            await foreach (var key in server.KeysAsync(pattern: $"{_prefix}:*"))
                await database.KeyDeleteAsync(key);
        }
    }

    private static DurableAction CreateAction(DateTimeOffset now)
        => new()
        {
            Key = Key,
            Lane = MuleSettings.DefaultLane,
            Payload = "{}",
            PayloadType = typeof(object).AssemblyQualifiedName,
            Status = DurableActionStatus.Pending,
            CreatedOnUtc = now
        };
}
