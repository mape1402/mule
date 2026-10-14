namespace Mule.FastLane.Redis.Tests;

using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Mule.FastLane.Redis;
using NSubstitute;
using StackExchange.Redis;
using System.Reflection;

[Collection("FastLane Redis")]
public sealed class RedisCoverageTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.fastlane.redis.coverage");

    [Fact]
    public void Envelope_Should_Roundtrip_With_Default_Lane()
    {
        var action = CreateAction("roundtrip", DateTimeOffset.UtcNow);
        action.Lane = " ";
        action.Status = DurableActionStatus.Completed;
        action.CompletedOnUtc = DateTimeOffset.UtcNow;
        action.TerminalOnUtc = action.CompletedOnUtc;

        var envelope = RedisActionEnvelope.FromAction(action);
        var restored = envelope.ToAction();

        Assert.Equal(MuleSettings.DefaultLane, envelope.Lane);
        Assert.Equal(MuleSettings.DefaultLane, restored.Lane);
        Assert.Equal(action.Id, restored.Id);
        Assert.Equal(action.CompletedOnUtc, restored.CompletedOnUtc);

        action.Lane = "named";
        Assert.Equal("named", RedisActionEnvelope.FromAction(action).ToAction().Lane);
    }

    [RedisFact]
    public async Task Buffer_Should_Run_Full_Redis_Lifecycle()
    {
        var prefix = $"mule-tests:{Guid.NewGuid():N}";
        await using var connection = await CreateConnectionAsync(prefix);
        var buffer = new RedisFastLaneBuffer(connection, Options.Create(new FastLaneRedisOptions
        {
            LeaseDuration = TimeSpan.FromMilliseconds(50),
            DeduplicationRetention = TimeSpan.Zero
        }));
        var now = DateTimeOffset.UtcNow;
        var action = CreateAction("one", now.AddSeconds(-3));
        var duplicate = CreateAction("duplicate", now, deduplicationKey: action.DeduplicationKey);
        var retry = CreateAction("retry", now.AddSeconds(-2));
        var future = CreateAction("future", now, nextAttemptOnUtc: now.AddMinutes(5));
        var active = CreateAction("active", now, DurableActionStatus.Locked, lockedOnUtc: now);
        var expired = CreateAction("expired", now.AddSeconds(-4), DurableActionStatus.Locked, lockedOnUtc: now.AddMinutes(-10));
        var lockOnly = CreateAction("lock-only", now.AddSeconds(-5));
        var otherLane = CreateAction("other-lane", now.AddSeconds(-6));
        otherLane.Lane = "Lane-B";

        try
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => buffer.TryAddAsync(null));
            await Assert.ThrowsAsync<ArgumentNullException>(() => buffer.TryAddRangeAsync(null));
            await Assert.ThrowsAsync<ArgumentException>(() => buffer.TryAddRangeAsync([null]));
            Assert.Empty(await buffer.TryAddRangeAsync([]));

            Assert.True(await buffer.TryAddAsync(action));
            Assert.Empty(await buffer.TryAddRangeAsync([duplicate]));
            Assert.Equal(action.Id, await buffer.FindByDeduplicationKeyAsync(Key, action.DeduplicationKey));
            Assert.Null(await buffer.FindByDeduplicationKeyAsync(Key, ""));
            Assert.Null(await buffer.LockAsync(Ulid.NewUlid(), TimeSpan.FromMinutes(1), now));

            await buffer.TryAddRangeAsync([retry, future, active, expired, lockOnly, otherLane]);
            Assert.NotNull(await buffer.LockAsync(expired.Id, TimeSpan.FromMinutes(1), now));
            Assert.NotNull(await buffer.LockAsync(lockOnly.Id, TimeSpan.FromMinutes(1), now));
            Assert.Null(await buffer.LockAsync(lockOnly.Id, TimeSpan.FromMinutes(1), now));
            var claimNow = DateTimeOffset.UtcNow.AddSeconds(1);
            var claimed = await buffer.ClaimPendingAsync("Lane-A", 10, TimeSpan.FromMinutes(1), claimNow);
            Assert.Contains(claimed, x => x.Id == action.Id);
            Assert.Contains(claimed, x => x.Id == retry.Id);
            Assert.DoesNotContain(claimed, x => x.Id == future.Id);
            Assert.DoesNotContain(claimed, x => x.Id == active.Id);
            Assert.NotNull(await buffer.GetNextPendingOnUtcAsync());

            Assert.True(await buffer.MarkFailedAsync(retry.Id, "again", now, now.AddMilliseconds(1)));
            await Task.Delay(60);
            var retryLock = await buffer.LockAsync(retry.Id, TimeSpan.FromMilliseconds(1), now.AddSeconds(1));
            Assert.NotNull(retryLock);
            Assert.True(await buffer.MarkFailedAsync(retry.Id, null, now.AddSeconds(2), null));
            Assert.False(await buffer.MarkFailedAsync(Ulid.NewUlid(), "missing", now, null));

            var intents = await buffer.TakeIntentFlushBatchAsync(1);
            Assert.Single(intents);
            await buffer.CancelIntentFlushAsync(intents.Select(x => x.Id).Append(Ulid.NewUlid()).ToArray());
            intents = await buffer.TakeIntentFlushBatchAsync(10);
            Assert.NotEmpty(intents);
            Assert.Contains(intents, x => x.Status == DurableActionStatus.Locked);
            await buffer.CompleteIntentFlushAsync([]);
            await buffer.CompleteIntentFlushAsync(intents.Select(x => x.Id).Append(Ulid.NewUlid()).ToArray());

            Assert.True(await buffer.MarkCompletedAsync(action.Id, now.AddSeconds(3)));
            Assert.False(await buffer.MarkCompletedAsync(Ulid.NewUlid(), now));
            var terminals = await buffer.TakeTerminalFlushBatchAsync(10);
            Assert.NotEmpty(terminals);
            await buffer.CancelIntentFlushAsync([]);
            await buffer.CancelTerminalFlushAsync([]);
            await buffer.CancelTerminalFlushAsync(terminals.Select(x => x.Id).Append(Ulid.NewUlid()).ToArray());
            terminals = await buffer.TakeTerminalFlushBatchAsync(10);
            await buffer.CompleteTerminalFlushAsync([]);
            await buffer.CompleteTerminalFlushAsync(terminals.Select(x => x.Id).Append(Ulid.NewUlid()).ToArray());

            var completedBeforeIntent = CreateAction("completed-before-intent", now, DurableActionStatus.Completed);
            completedBeforeIntent.Attempts = 3;
            var failedBeforeIntent = CreateAction("failed-before-intent", now, DurableActionStatus.Failed);
            failedBeforeIntent.Attempts = 3;
            await buffer.TryAddRangeAsync([completedBeforeIntent, failedBeforeIntent]);
            var durableIntents = await buffer.TakeIntentFlushBatchAsync(10);
            Assert.Contains(durableIntents, x => x.Id == completedBeforeIntent.Id && x.Status == DurableActionStatus.Locked);
            Assert.Contains(durableIntents, x => x.Id == failedBeforeIntent.Id && x.Attempts == 2);
        }
        finally
        {
            await CleanupRedisAsync(prefix);
        }
    }

    [RedisFact]
    public async Task Storage_Should_Combine_Buffer_And_Durable_Storage()
    {
        var prefix = $"mule-tests:{Guid.NewGuid():N}";
        await using var connection = await CreateConnectionAsync(prefix);
        var buffer = new RedisFastLaneBuffer(connection, Options.Create(new FastLaneRedisOptions()));
        var durable = new RecordingDurableStorage();
        var storage = new FastLaneRedisStorage(durable, buffer);
        var now = DateTimeOffset.UtcNow;
        var buffered = CreateAction("buffered", now);
        var durableOnly = CreateAction("durable", now);
        durable.DeduplicatedId = durableOnly.Id;
        durable.NextPending = now;
        durable.Pending.Add(durableOnly);

        try
        {
            Assert.Throws<ArgumentNullException>(() => new FastLaneRedisStorage(null, buffer));
            Assert.Throws<ArgumentNullException>(() => new FastLaneRedisStorage(durable, null));
            await Assert.ThrowsAsync<ArgumentNullException>(() => storage.AddAsync(null));
            await Assert.ThrowsAsync<ArgumentNullException>(() => storage.AddRangeAsync(null));
            await Assert.ThrowsAsync<ArgumentException>(() => storage.AddRangeAsync([null]));

            await storage.AddAsync(buffered);
            await storage.AddRangeAsync([CreateAction("skip", now, deduplicationKey: "durable-dedupe")]);
            Assert.Equal(now, await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now));
            await storage.SaveChangesAsync();

            durable.NextPending = now.AddMinutes(1);
            Assert.True(await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now) < durable.NextPending);
            durable.NextPending = now;

            Assert.Equal(buffered.Id, await storage.FindByDeduplicationKeyAsync(Key, buffered.DeduplicationKey));
            Assert.Null(await storage.FindByDeduplicationKeyAsync(Key, ""));
            Assert.Equal(durable.DeduplicatedId, await storage.FindByDeduplicationKeyAsync(Key, "durable-dedupe"));
            durable.NextPending = null;
            Assert.NotNull(await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now));
            durable.NextPending = now;

            var claimNow = DateTimeOffset.UtcNow.AddSeconds(1);
            var claimed = await storage.ClaimPendingAsync("Lane-A", 5, TimeSpan.FromMinutes(1), claimNow);
            Assert.Contains(claimed, x => x.Id == buffered.Id);
            Assert.Contains(claimed, x => x.Id == durableOnly.Id);
            Assert.NotNull(await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now));
            Assert.Equal(durableOnly.Id, (await storage.LockAsync(durableOnly.Id, TimeSpan.FromMinutes(1), now)).Id);

            var shortCircuit = CreateAction("short-circuit", now.AddSeconds(-4));
            await storage.AddAsync(shortCircuit);
            await storage.SaveChangesAsync();
            Assert.Single(await storage.ClaimPendingAsync("Lane-A", 1, TimeSpan.FromMinutes(1), claimNow));

            durable.NextPending = now.AddMinutes(-1);
            Assert.Equal(durable.NextPending, await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now));
            await storage.MarkCompletedAsync(buffered.Id, now);
            await storage.MarkCompletedAsync(durableOnly.Id, now);
            await storage.MarkFailedAsync(buffered.Id, "buffered", now, null);
            await storage.MarkFailedAsync(durableOnly.Id, "durable", now, null);
            Assert.Equal(1, await storage.CleanCompletedAsync(now, 10));
            Assert.NotNull(await storage.GetOldestCompletedOnUtcAsync());
            await storage.SaveChangesAsync();

            Assert.True(durable.MarkCompletedCalled);
            Assert.True(durable.MarkFailedCalled);
        }
        finally
        {
            await CleanupRedisAsync(prefix);
        }
    }

    [RedisFact]
    public async Task GetNextPendingOnUtcAsync_Should_Return_Earlier_Durable_Due_Time()
    {
        var prefix = $"mule-tests:{Guid.NewGuid():N}";
        await using var connection = await CreateConnectionAsync(prefix);
        var durable = new RecordingDurableStorage();
        var storage = new FastLaneRedisStorage(
            durable,
            new RedisFastLaneBuffer(connection, Options.Create(new FastLaneRedisOptions())));
        var now = DateTimeOffset.UtcNow;
        durable.NextPending = now.AddMinutes(-1);

        try
        {
            await storage.AddAsync(CreateAction("buffered-later", now));
            await storage.SaveChangesAsync();

            Assert.Equal(durable.NextPending, await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now));

            durable.NextPending = now.AddMinutes(1);
            Assert.True(await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now) < durable.NextPending);
        }
        finally
        {
            await CleanupRedisAsync(prefix);
        }
    }

    [RedisFact]
    public async Task LockAsync_Should_Return_Null_When_Lease_Is_Held()
    {
        var prefix = $"mule-tests:{Guid.NewGuid():N}";
        await using var connection = await CreateConnectionAsync(prefix);
        var buffer = new RedisFastLaneBuffer(connection, Options.Create(new FastLaneRedisOptions()));
        var action = CreateAction("leased", DateTimeOffset.UtcNow);

        try
        {
            Assert.True(await buffer.TryAddAsync(action));
            var database = await connection.GetDatabaseAsync();
            await database.StringSetAsync(
                $"{prefix}:fastlane:lease:{action.Id}",
                "another-replica",
                TimeSpan.FromMinutes(1));

            Assert.Null(await buffer.LockAsync(action.Id, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow));
        }
        finally
        {
            await CleanupRedisAsync(prefix);
        }
    }

    [RedisFact]
    public async Task FlushService_Should_Flush_Intents_And_Terminals_In_Order()
    {
        var prefix = $"mule-tests:{Guid.NewGuid():N}";
        await using var connection = await CreateConnectionAsync(prefix);
        var buffer = new RedisFastLaneBuffer(connection, Options.Create(new FastLaneRedisOptions()));
        var durable = new RecordingDurableStorage();
        var service = CreateFlushService(buffer, durable);
        var now = DateTimeOffset.UtcNow;
        var completed = CreateAction("completed", now);
        var failed = CreateAction("failed", now);

        try
        {
            await buffer.TryAddRangeAsync([completed, failed]);
            await InvokeFlushOnceAsync(service);
            Assert.Equal(2, durable.Added.Count);

            Assert.True(await buffer.MarkCompletedAsync(completed.Id, now.AddSeconds(1)));
            Assert.True(await buffer.MarkFailedAsync(failed.Id, "terminal", now.AddSeconds(2), null));
            await InvokeFlushOnceAsync(service);

            Assert.True(durable.MarkCompletedRangeCalled);
            Assert.True(durable.MarkFailedCalled);

            var nullCompleted = CreateAction("null-completed-date", now);
            var nullFailed = CreateAction("null-terminal-date", now);
            await buffer.TryAddRangeAsync([nullCompleted, nullFailed]);
            await InvokeFlushOnceAsync(service);
            Assert.True(await buffer.MarkCompletedAsync(nullCompleted.Id, now));
            Assert.True(await buffer.MarkFailedAsync(nullFailed.Id, "failed", now, null));
            await ClearTerminalDatesAsync(connection, nullCompleted.Id, nullFailed.Id);
            await InvokeFlushOnceAsync(service);

            var failingDurable = new RecordingDurableStorage { ThrowOnAdd = true };
            var failingService = CreateFlushService(buffer, failingDurable);
            var failingIntent = CreateAction("failing-intent", now);
            await buffer.TryAddAsync(failingIntent);
            await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeFlushOnceAsync(failingService));

            failingDurable.ThrowOnAdd = false;
            await InvokeFlushOnceAsync(failingService);
            Assert.True(await buffer.MarkFailedAsync(failingIntent.Id, "terminal", now.AddSeconds(3), null));
            failingDurable.ThrowOnMarkFailed = true;
            await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeFlushOnceAsync(failingService));

            using var running = CreateFlushService(buffer, durable, TimeSpan.Zero);
            await running.StartAsync(CancellationToken.None);
            await running.StopAsync(CancellationToken.None);

            var loopingDurable = new RecordingDurableStorage { ThrowOnAdd = true };
            var loopingService = CreateFlushService(buffer, loopingDurable, TimeSpan.FromMilliseconds(1));
            await buffer.TryAddAsync(CreateAction("loop-failure", now));
            await loopingService.StartAsync(CancellationToken.None);
            await Task.Delay(20);
            await loopingService.StopAsync(CancellationToken.None);
            loopingService.Dispose();

            var blockingDurable = new RecordingDurableStorage { BlockOnAdd = true };
            var blockingAction = CreateAction("loop-cancellation", now);
            await buffer.TryAddAsync(blockingAction);
            using var canceledLoop = CreateFlushService(buffer, blockingDurable, TimeSpan.FromSeconds(1));
            await canceledLoop.StartAsync(CancellationToken.None);
            await Task.Delay(20);
            await canceledLoop.StopAsync(CancellationToken.None);

            var nonBatch = Substitute.For<IMuleDurableStorage>();
            nonBatch.AddRangeAsync(Arg.Any<IReadOnlyCollection<DurableAction>>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
            nonBatch.SaveChangesAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
            nonBatch.MarkCompletedAsync(Arg.Any<Ulid>(), Arg.Any<DateTimeOffset>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
            var nonBatchAction = CreateAction("non-batch", now);
            await buffer.TryAddAsync(nonBatchAction);
            var nonBatchService = CreateFlushService(buffer, nonBatch);
            await InvokeFlushOnceAsync(nonBatchService);
            Assert.True(await buffer.MarkCompletedAsync(nonBatchAction.Id, now));
            await InvokeFlushOnceAsync(nonBatchService);
            await nonBatch.Received().MarkCompletedAsync(nonBatchAction.Id, Arg.Any<DateTimeOffset>(), Arg.Any<CancellationToken>());

            var nonBatchNullDate = CreateAction("non-batch-null-date", now);
            await buffer.TryAddAsync(nonBatchNullDate);
            await InvokeFlushOnceAsync(nonBatchService);
            Assert.True(await buffer.MarkCompletedAsync(nonBatchNullDate.Id, now));
            await ClearCompletedDateAsync(connection, nonBatchNullDate.Id);
            await InvokeFlushOnceAsync(nonBatchService);
        }
        finally
        {
            await CleanupRedisAsync(prefix);
        }
    }

    [Fact]
    public void Registration_Connection_And_FlushService_Should_Validate_Nulls()
    {
        Assert.Throws<ArgumentNullException>(() => MuleFastLaneRedisRegistrationBuilderExtensions.UseFastLaneRedis(null, _ => { }));
        var builder = Substitute.For<Mule.Configuration.IMuleRegistrationBuilder>();
        Assert.Throws<ArgumentNullException>(() => builder.UseFastLaneRedis(null));
        Assert.Throws<ArgumentNullException>(() => new RedisFastLaneConnection(null, new ServiceCollection().BuildServiceProvider()));
        Assert.Throws<ArgumentNullException>(() => new RedisFastLaneConnection(Options.Create(new FastLaneRedisOptions()), null));
        Assert.Throws<ArgumentNullException>(() => new RedisFastLaneBuffer(null, Options.Create(new FastLaneRedisOptions())));
        Assert.Throws<ArgumentNullException>(() => new RedisFastLaneBuffer(CreateUnopenedConnection(), null));
        Assert.Throws<ArgumentNullException>(() => new FastLaneRedisFlushService(null, CreateRedisBuffer(), Options.Create(new FastLaneRedisOptions()), NullLogger<FastLaneRedisFlushService>.Instance));
        Assert.Throws<ArgumentNullException>(() => new FastLaneRedisFlushService(new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(), null, Options.Create(new FastLaneRedisOptions()), NullLogger<FastLaneRedisFlushService>.Instance));
        Assert.Throws<ArgumentNullException>(() => new FastLaneRedisFlushService(new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(), CreateRedisBuffer(), null, NullLogger<FastLaneRedisFlushService>.Instance));
        Assert.Throws<ArgumentNullException>(() => new FastLaneRedisFlushService(new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(), CreateRedisBuffer(), Options.Create(new FastLaneRedisOptions()), null));

        var options = new FastLaneRedisOptions
        {
            KeyPrefix = "prefix:",
            IntentFlushSize = 0,
            CompletionFlushSize = 0,
            FlushInterval = TimeSpan.Zero,
            LeaseDuration = TimeSpan.Zero,
            DeduplicationRetention = TimeSpan.Zero
        };
        Assert.Equal("prefix:", options.KeyPrefix);
    }

    [RedisFact]
    public async Task Connection_Should_Support_Default_Prefix_Factory_Ownership_And_Synchronous_Dispose()
    {
        var multiplexer = Substitute.For<IConnectionMultiplexer>();
        multiplexer.GetDatabase(Arg.Any<int>(), Arg.Any<object>()).Returns(Substitute.For<IDatabase>());
        var connection = new RedisFastLaneConnection(
            Options.Create(new FastLaneRedisOptions
            {
                KeyPrefix = " ",
                DisposeConnection = true,
                ConnectionFactory = (_, _) => ValueTask.FromResult(multiplexer)
            }),
            new ServiceCollection().BuildServiceProvider());

        Assert.Equal("mule", connection.Prefix);
        _ = await connection.GetDatabaseAsync();
        _ = await connection.GetDatabaseAsync();
        connection.Dispose();

        multiplexer.Received(1).Dispose();

        var nullFactory = new RedisFastLaneConnection(
            Options.Create(new FastLaneRedisOptions
            {
                DisposeConnection = true,
                ConnectionFactory = (_, _) => ValueTask.FromResult<IConnectionMultiplexer>(null)
            }),
            new ServiceCollection().BuildServiceProvider());
        await Assert.ThrowsAsync<InvalidOperationException>(() => nullFactory.GetDatabaseAsync().AsTask());
        nullFactory.Dispose();

        await using var defaultConnection = new RedisFastLaneConnection(
            Options.Create(new FastLaneRedisOptions { ConnectionString = " " }),
            new ServiceCollection().BuildServiceProvider());
        _ = await defaultConnection.GetDatabaseAsync();
    }

    [Fact]
    public async Task Connection_Should_Reuse_A_Connection_Created_While_Another_Caller_Waits()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var multiplexer = Substitute.For<IConnectionMultiplexer>();
        multiplexer.GetDatabase(Arg.Any<int>(), Arg.Any<object>()).Returns(Substitute.For<IDatabase>());
        await using var connection = new RedisFastLaneConnection(
            Options.Create(new FastLaneRedisOptions
            {
                ConnectionFactory = async (_, _) =>
                {
                    entered.TrySetResult();
                    await release.Task;
                    return multiplexer;
                }
            }),
            new ServiceCollection().BuildServiceProvider());

        var first = connection.GetDatabaseAsync().AsTask();
        await entered.Task;
        var second = connection.GetDatabaseAsync().AsTask();
        release.TrySetResult();

        await Task.WhenAll(first, second);
    }

    [Fact]
    public void Buffer_Helpers_Should_Cover_Every_Lock_State_And_Option_Default()
    {
        var now = DateTimeOffset.UtcNow;
        Assert.True(InvokeCanLock(CreateAction("pending-null", now), now));
        Assert.True(InvokeCanLock(CreateAction("pending-due", now, nextAttemptOnUtc: now.AddSeconds(-1)), now));
        Assert.False(InvokeCanLock(CreateAction("pending-future", now, nextAttemptOnUtc: now.AddSeconds(1)), now));
        Assert.True(InvokeCanLock(CreateAction("locked-expired", now, DurableActionStatus.Locked, now.AddMinutes(-2)), now));
        Assert.False(InvokeCanLock(CreateAction("locked-active", now, DurableActionStatus.Locked, now), now));
        Assert.False(InvokeCanLock(CreateAction("locked-null", now, DurableActionStatus.Locked), now));
        Assert.False(InvokeCanLock(CreateAction("completed", now, DurableActionStatus.Completed), now));

        var normalize = typeof(RedisFastLaneBuffer).GetMethod("NormalizeLane", BindingFlags.Static | BindingFlags.NonPublic);
        Assert.Equal(MuleSettings.DefaultLane, normalize.Invoke(null, [" "]));
        Assert.Equal("lane", normalize.Invoke(null, ["lane"]));

        var zeroOptionsBuffer = new RedisFastLaneBuffer(
            CreateUnopenedConnection(),
            Options.Create(new FastLaneRedisOptions { LeaseDuration = TimeSpan.Zero }));
        var getLease = typeof(RedisFastLaneBuffer).GetMethod("GetLeaseDuration", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.Equal(TimeSpan.FromMinutes(2), getLease.Invoke(zeroOptionsBuffer, null));
    }

    private static bool InvokeCanLock(DurableAction action, DateTimeOffset now)
    {
        var method = typeof(RedisFastLaneBuffer).GetMethod("CanLock", BindingFlags.Static | BindingFlags.NonPublic);
        return (bool)method.Invoke(null, [action, TimeSpan.FromMinutes(1), now]);
    }

    private static RedisFastLaneBuffer CreateRedisBuffer()
        => new(CreateUnopenedConnection(), Options.Create(new FastLaneRedisOptions()));

    private static FastLaneRedisFlushService CreateFlushService(
        RedisFastLaneBuffer buffer,
        IMuleDurableStorage durable,
        TimeSpan? flushInterval = null)
    {
        var services = new ServiceCollection()
            .AddSingleton<IMuleDurableStorage>(durable)
            .BuildServiceProvider();

        return new FastLaneRedisFlushService(
            services.GetRequiredService<IServiceScopeFactory>(),
            buffer,
            Options.Create(new FastLaneRedisOptions
            {
                IntentFlushSize = 10,
                CompletionFlushSize = 10,
                FlushInterval = flushInterval ?? TimeSpan.Zero
            }),
            NullLogger<FastLaneRedisFlushService>.Instance);
    }

    private static async Task InvokeFlushOnceAsync(FastLaneRedisFlushService service)
    {
        var method = typeof(FastLaneRedisFlushService).GetMethod(
            "FlushOnceAsync",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var task = (Task)method.Invoke(service, [CancellationToken.None]);
        await task;
    }

    private static RedisFastLaneConnection CreateUnopenedConnection()
        => new(Options.Create(new FastLaneRedisOptions
        {
            ConnectionFactory = (_, _) => throw new InvalidOperationException("not used")
        }), new ServiceCollection().BuildServiceProvider());

    private static async Task<RedisFastLaneConnection> CreateConnectionAsync(string prefix)
    {
        var options = Options.Create(new FastLaneRedisOptions
        {
            ConnectionString = Environment.GetEnvironmentVariable(RedisFastLaneTests.ConnectionStringEnvironmentVariable),
            KeyPrefix = prefix,
            LeaseDuration = TimeSpan.FromMilliseconds(100),
            DeduplicationRetention = TimeSpan.FromDays(1)
        });
        var connection = new RedisFastLaneConnection(options, new ServiceCollection().BuildServiceProvider());
        _ = await connection.GetDatabaseAsync();
        return connection;
    }

    private static async Task CleanupRedisAsync(string keyPrefix)
    {
        var connectionString = Environment.GetEnvironmentVariable(RedisFastLaneTests.ConnectionStringEnvironmentVariable);
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

    private static async Task ClearTerminalDatesAsync(
        RedisFastLaneConnection connection,
        Ulid completedId,
        Ulid failedId)
    {
        var database = await connection.GetDatabaseAsync();
        foreach (var (id, clearCompleted) in new[] { (completedId, true), (failedId, false) })
        {
            var key = $"{connection.Prefix}:fastlane:action:{id}";
            var value = await database.StringGetAsync(key);
            var envelope = JsonSerializer.Deserialize<RedisActionEnvelope>((string)value, new JsonSerializerOptions(JsonSerializerDefaults.Web));
            if (clearCompleted)
                envelope.CompletedOnUtc = null;
            else
                envelope.TerminalOnUtc = null;
            await database.StringSetAsync(key, JsonSerializer.Serialize(envelope, new JsonSerializerOptions(JsonSerializerDefaults.Web)));
        }
    }

    private static async Task ClearCompletedDateAsync(RedisFastLaneConnection connection, Ulid id)
    {
        var database = await connection.GetDatabaseAsync();
        var key = $"{connection.Prefix}:fastlane:action:{id}";
        var value = await database.StringGetAsync(key);
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        var envelope = JsonSerializer.Deserialize<RedisActionEnvelope>((string)value, options);
        envelope.CompletedOnUtc = null;
        await database.StringSetAsync(key, JsonSerializer.Serialize(envelope, options));
    }

    private static DurableAction CreateAction(
        string value,
        DateTimeOffset createdOnUtc,
        DurableActionStatus status = DurableActionStatus.Pending,
        DateTimeOffset? lockedOnUtc = null,
        DateTimeOffset? nextAttemptOnUtc = null,
        string deduplicationKey = null)
        => new()
        {
            Key = Key,
            Lane = "Lane-A",
            Payload = $"\"{value}\"",
            PayloadType = typeof(string).AssemblyQualifiedName,
            DeduplicationKey = deduplicationKey ?? value,
            Status = status,
            CreatedOnUtc = createdOnUtc,
            LockedOnUtc = lockedOnUtc,
            StartedOnUtc = lockedOnUtc,
            NextAttemptOnUtc = nextAttemptOnUtc,
            Attempts = status == DurableActionStatus.Failed ? 1 : 0,
            LastError = status == DurableActionStatus.Failed ? "failed" : null
        };

    private sealed class RecordingDurableStorage : IMuleDurableStorage, IMuleBatchTerminalStorage
    {
        public List<DurableAction> Added { get; } = new();
        public List<DurableAction> Pending { get; } = new();
        public Ulid? DeduplicatedId { get; set; }
        public DateTimeOffset? NextPending { get; set; } = DateTimeOffset.UtcNow;
        public bool MarkCompletedCalled { get; private set; }
        public bool MarkCompletedRangeCalled { get; private set; }
        public bool MarkFailedCalled { get; private set; }
        public bool ThrowOnAdd { get; set; }
        public bool BlockOnAdd { get; set; }
        public bool ThrowOnMarkFailed { get; set; }

        public Task AddAsync(DurableAction action, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public async Task AddRangeAsync(IReadOnlyCollection<DurableAction> actions, CancellationToken cancellationToken = default)
        {
            if (BlockOnAdd)
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);

            if (ThrowOnAdd)
                throw new InvalidOperationException("add failed");

            Added.AddRange(actions);
        }

        public Task<Ulid?> FindByDeduplicationKeyAsync(ActionKey key, string deduplicationKey, CancellationToken cancellationToken = default)
            => Task.FromResult(deduplicationKey == "durable-dedupe" ? DeduplicatedId : null);

        public Task<IReadOnlyCollection<DurableAction>> ClaimPendingAsync(string lane, int batchSize, TimeSpan lockTimeout, DateTimeOffset now, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyCollection<DurableAction>>(Pending.Take(batchSize).ToArray());

        public Task<DateTimeOffset?> GetNextPendingOnUtcAsync(TimeSpan lockTimeout, DateTimeOffset now, CancellationToken cancellationToken = default)
            => Task.FromResult(NextPending);

        public Task<DurableAction> LockAsync(Ulid id, TimeSpan lockTimeout, DateTimeOffset now, CancellationToken cancellationToken = default)
            => Task.FromResult(Pending.SingleOrDefault(x => x.Id == id));

        public Task MarkCompletedAsync(Ulid id, DateTimeOffset completedOnUtc, CancellationToken cancellationToken = default)
        {
            MarkCompletedCalled = true;
            return Task.CompletedTask;
        }

        public Task MarkCompletedRangeAsync(IReadOnlyCollection<MuleCompletedAction> actions, CancellationToken cancellationToken = default)
        {
            MarkCompletedRangeCalled = true;
            return Task.CompletedTask;
        }

        public Task MarkFailedAsync(Ulid id, string error, DateTimeOffset now, DateTimeOffset? nextAttemptOnUtc, CancellationToken cancellationToken = default)
        {
            if (ThrowOnMarkFailed)
                throw new InvalidOperationException("mark failed");

            MarkFailedCalled = true;
            return Task.CompletedTask;
        }

        public Task<int> CleanCompletedAsync(DateTimeOffset olderThanUtc, int batchSize, CancellationToken cancellationToken = default)
            => Task.FromResult(1);

        public Task<DateTimeOffset?> GetOldestCompletedOnUtcAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<DateTimeOffset?>(DateTimeOffset.UtcNow);

        public Task SaveChangesAsync(CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }
}
