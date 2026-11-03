namespace Mule.Redis.Tests;

using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Mule.Diagnostics;
using NSubstitute;
using StackExchange.Redis;

[Collection("Redis durable storage")]
public sealed class RedisCoverageTests : IAsyncLifetime
{
    private static readonly ActionKey Key = ActionKey.From("tests.redis.coverage.v1");
    private readonly string _prefix = $"mule-redis-coverage-{Guid.NewGuid():N}";

    [Fact]
    public void Codecs_Should_Handle_Empty_And_Complete_Values()
    {
        var keys = new RedisMuleKeyspace("coverage");
        var now = DateTimeOffset.UtcNow;
        var action = NewAction();
        action.Status = DurableActionStatus.Locked;
        action.LockedOnUtc = now;
        action.StartedOnUtc = now;
        action.NextAttemptOnUtc = now.AddSeconds(1);
        action.CompletedOnUtc = now.AddSeconds(2);
        action.CancelledOnUtc = now.AddSeconds(3);
        action.TerminalOnUtc = now.AddSeconds(4);
        action.LastError = "error";
        action.Metadata = "{}";
        action.CorrelationId = "correlation";
        action.DeduplicationKey = "dedupe";

        var arguments = RedisMuleActionCodec.ToArguments(action, keys, TimeSpan.FromMinutes(1));
        var hash = ActionHash(action, keys);
        var roundTrip = RedisMuleActionCodec.FromHash(hash);

        Assert.Equal(RedisMuleActionCodec.ArgumentCount, arguments.Length);
        Assert.Equal(action.Id, roundTrip.Id);
        Assert.Equal(action.LockedOnUtc.Value.ToUnixTimeMilliseconds(), roundTrip.LockedOnUtc.Value.ToUnixTimeMilliseconds());
        Assert.Equal(action.TerminalOnUtc.Value.ToUnixTimeMilliseconds(), roundTrip.TerminalOnUtc.Value.ToUnixTimeMilliseconds());
        Assert.Null(RedisMuleActionCodec.FromHash(null));
        Assert.Null(RedisMuleActionCodec.FromHash([]));
        Assert.Throws<InvalidOperationException>(() => RedisMuleActionCodec.FromHash([new HashEntry("id", action.Id.ToString())]));
    }

    [Fact]
    public void Action_Codec_Should_Default_Missing_Optional_Values()
    {
        var keys = new RedisMuleKeyspace("coverage-defaults");
        var now = DateTimeOffset.UtcNow;
        var action = new DurableAction
        {
            Key = default,
            Lane = " ",
            Status = DurableActionStatus.Locked,
            CreatedOnUtc = now
        };

        var arguments = RedisMuleActionCodec.ToArguments(action, keys, TimeSpan.FromSeconds(10));
        var pendingArguments = RedisMuleActionCodec.ToArguments(
            new DurableAction
            {
                Key = Key,
                CreatedOnUtc = now,
                NextAttemptOnUtc = now.AddMinutes(1)
            },
            keys,
            TimeSpan.FromSeconds(10));
        var minimal = RedisMuleActionCodec.FromHash(
        [
            new("id", action.Id.ToString()),
            new("ct", action.ConcurrencyToken.ToString()),
            new("key", Key.Value),
            new("lane", RedisValue.Null),
            new("payload", ""),
            new("status", "invalid"),
            new("attempts", "invalid"),
            new("created", "invalid")
        ]);

        Assert.Equal(MuleSettings.DefaultLane, arguments[3].ToString());
        Assert.Equal(string.Empty, arguments[5].ToString());
        Assert.Equal(string.Empty, arguments[10].ToString());
        Assert.Equal(now.AddSeconds(10).ToUnixTimeMilliseconds(), (long)arguments[21]);
        Assert.Equal(now.AddMinutes(1).ToUnixTimeMilliseconds(), (long)pendingArguments[21]);
        Assert.Equal(MuleSettings.DefaultLane, minimal.Lane);
        Assert.Equal(DateTimeOffset.UnixEpoch, minimal.CreatedOnUtc);
        Assert.Equal(0, minimal.Attempts);
    }

    [Fact]
    public void Replay_Codec_Should_Canonicalize_Metadata_And_Handle_Empty_Values()
    {
        var metadata = RedisMuleReplayCodec.SerializeMetadata(new Dictionary<string, string>
        {
            ["z"] = "last",
            ["a"] = "first"
        });
        var replay = new MuleReplay
        {
            RequestId = Ulid.NewUlid(),
            SourceActionId = Ulid.NewUlid(),
            ReplayRootActionId = Ulid.NewUlid(),
            ReplayActionId = Ulid.NewUlid(),
            BranchId = Ulid.Empty,
            ActionKey = Key,
            RequestedOnUtc = DateTimeOffset.UtcNow,
            ScheduleOnUtc = DateTimeOffset.UtcNow,
            Metadata = metadata
        };

        var info = RedisMuleReplayCodec.ToInfo(replay);

        Assert.StartsWith("{\"a\"", metadata);
        Assert.Equal("first", info.Metadata["a"]);
        Assert.Null(RedisMuleReplayCodec.SerializeMetadata(null));
        Assert.Null(RedisMuleReplayCodec.SerializeMetadata(new Dictionary<string, string>()));
        Assert.Null(RedisMuleReplayCodec.FromHash(null));
        Assert.Null(RedisMuleReplayCodec.ToInfo(null));
    }

    [Fact]
    public void Replay_Codec_Should_Roundtrip_Complete_And_Defaulted_Values()
    {
        var now = DateTimeOffset.UtcNow;
        var id = Ulid.NewUlid();
        var full = RedisMuleReplayCodec.FromHash(ReplayHash(id, now, "{}", Ulid.NewUlid().ToString()));
        var minimal = RedisMuleReplayCodec.FromHash(
        [
            new("id", id.ToString()), new("ct", Ulid.NewUlid().ToString()),
            new("source", Ulid.NewUlid().ToString()), new("root", Ulid.NewUlid().ToString()),
            new("action", Ulid.NewUlid().ToString()), new("branch", Ulid.Empty.ToString()),
            new("key", Key.Value), new("lane", RedisValue.Null), new("correlation", ""),
            new("sequence", "invalid"), new("status", "invalid"),
            new("requested", "invalid"), new("schedule", "invalid"),
            new("attempts", "invalid"), new("lockToken", "invalid")
        ]);
        var nullMetadata = RedisMuleReplayCodec.ToInfo(new MuleReplay { Metadata = "null" });
        var emptyMetadata = RedisMuleReplayCodec.ToInfo(new MuleReplay());

        Assert.Equal(id, full.RequestId);
        Assert.NotEqual(Ulid.Empty, full.LockToken);
        Assert.Equal(now.ToUnixTimeMilliseconds(), full.MaterializedOnUtc.Value.ToUnixTimeMilliseconds());
        Assert.Equal(DateTimeOffset.UnixEpoch, minimal.RequestedOnUtc);
        Assert.Equal(Ulid.Empty, minimal.LockToken);
        Assert.Empty(nullMetadata.Metadata);
        Assert.Empty(emptyMetadata.Metadata);
        Assert.Throws<InvalidOperationException>(() => RedisMuleReplayCodec.FromHash([new HashEntry("id", id.ToString())]));
    }

    [Fact]
    public void Registration_Should_Reject_Null_And_Multiple_Durable_Providers()
    {
        Assert.Throws<ArgumentNullException>(() => MuleRedisRegistrationBuilderExtensions.UseRedis(null));

        var services = new ServiceCollection();
        Assert.Throws<InvalidOperationException>(() => services.AddMule(mule => mule.UseRedis().UseRedis()));
    }

    [Fact]
    public void Connection_Should_Validate_Constructor_And_Normalize_Options()
    {
        var provider = new ServiceCollection().BuildServiceProvider();
        Assert.Throws<ArgumentNullException>(() => new RedisMuleConnection(null, provider));
        Assert.Throws<ArgumentNullException>(() => new RedisMuleConnection(Options.Create(new RedisMuleOptions()), null));

        using var connection = new RedisMuleConnection(
            Options.Create(new RedisMuleOptions { WriteBatchSize = 0, KeyPrefix = " " }),
            provider);
        Assert.Equal(1, connection.WriteBatchSize);
        Assert.Contains("{mule-", connection.Keyspace.Root);
        Assert.Equal(connection.Keyspace.Root, new RedisMuleKeyspace("mule").Root);
    }

    [Fact]
    public void Runtime_Components_Should_Validate_Constructor_Dependencies()
    {
        using var provider = new ServiceCollection().BuildServiceProvider();
        using var connection = new RedisMuleConnection(Options.Create(new RedisMuleOptions()), provider);
        var metrics = new MuleRuntimeMetrics();
        var settings = Options.Create(new MuleSettings { LockTimeout = TimeSpan.Zero });

        Assert.Throws<ArgumentNullException>(() => new RedisMuleStorage(null, metrics, settings));
        Assert.Throws<ArgumentNullException>(() => new RedisMuleStorage(connection, null, settings));
        Assert.Throws<ArgumentNullException>(() => new RedisMuleStorage(connection, metrics, null));
        Assert.Throws<ArgumentNullException>(() => new RedisMuleDiagnostics(null, metrics, settings));
        Assert.Throws<ArgumentNullException>(() => new RedisMuleDiagnostics(connection, null, settings));
        Assert.Throws<ArgumentNullException>(() => new RedisMuleDiagnostics(connection, metrics, null));
        Assert.NotNull(new RedisMuleStorage(connection, metrics, settings));
        Assert.NotNull(new RedisMuleDiagnostics(connection, metrics, settings));
    }

    [Fact]
    public void Owned_Synchronous_Connection_Should_Be_Disposed()
    {
        var multiplexer = Substitute.For<IConnectionMultiplexer>();
        var provider = new ServiceCollection().BuildServiceProvider();
        var connection = new RedisMuleConnection(Options.Create(new RedisMuleOptions()), provider);
        typeof(RedisMuleConnection).GetField("_connection", BindingFlags.Instance | BindingFlags.NonPublic).SetValue(connection, multiplexer);
        typeof(RedisMuleConnection).GetField("_disposeConnection", BindingFlags.Instance | BindingFlags.NonPublic).SetValue(connection, true);

        connection.Dispose();

        multiplexer.Received(1).Dispose();
    }

    [Fact]
    public async Task Replication_Acknowledgement_Should_Respect_Configuration()
    {
        var database = Substitute.For<IDatabase>();
        database.ExecuteAsync("WAIT", Arg.Any<object[]>()).Returns(RedisResult.Create((RedisValue)0));
        var provider = new ServiceCollection().BuildServiceProvider();
        await using var disabled = new RedisMuleConnection(Options.Create(new RedisMuleOptions()), provider);
        await disabled.AcknowledgeReplicationAsync(database);

        await using var required = new RedisMuleConnection(
            Options.Create(new RedisMuleOptions
            {
                MinimumReplicaAcknowledgements = 1,
                ReplicaAcknowledgementTimeout = TimeSpan.Zero
            }),
            provider);
        await Assert.ThrowsAsync<TimeoutException>(() => required.AcknowledgeReplicationAsync(database));

        database.ExecuteAsync("WAIT", Arg.Any<object[]>()).Returns(RedisResult.Create((RedisValue)1));
        await using var acknowledged = new RedisMuleConnection(
            Options.Create(new RedisMuleOptions
            {
                MinimumReplicaAcknowledgements = 1,
                ReplicaAcknowledgementTimeout = TimeSpan.FromMilliseconds(50)
            }),
            provider);
        await acknowledged.AcknowledgeReplicationAsync(database);
    }

    [RedisFact]
    public async Task Storage_Should_Validate_Arguments_And_Empty_State()
    {
        using var provider = CreateProvider();
        using var scope = provider.CreateScope();
        var storage = (RedisMuleStorage)scope.ServiceProvider.GetRequiredService<IMuleStorage>();

        await Assert.ThrowsAsync<ArgumentNullException>(() => storage.AddAsync(null));
        await Assert.ThrowsAsync<ArgumentNullException>(() => storage.AddRangeAsync(null));
        await Assert.ThrowsAsync<ArgumentException>(() => storage.AddRangeAsync([null]));
        await Assert.ThrowsAsync<ArgumentNullException>(() => storage.MarkCompletedRangeAsync(null));
        await storage.MarkCompletedRangeAsync([]);
        await storage.SaveChangesAsync();
        Assert.Null(await storage.FindByDeduplicationKeyAsync(Key, null));
        Assert.Null(await storage.GetNextPendingOnUtcAsync(TimeSpan.Zero, DateTimeOffset.UtcNow));
        Assert.Null(await storage.GetOldestCompletedOnUtcAsync());
        Assert.Null(await storage.LockAsync(Ulid.NewUlid(), TimeSpan.Zero, DateTimeOffset.UtcNow));
        await Assert.ThrowsAsync<InvalidOperationException>(() => storage.MarkFailedAsync(
            Ulid.NewUlid(), "missing", DateTimeOffset.UtcNow, null));

        var database = await scope.ServiceProvider.GetRequiredService<RedisMuleConnection>().GetDatabaseAsync();
        var keys = new RedisMuleKeyspace(_prefix);
        await database.HashSetAsync(keys.ActionDedupe, keys.DedupeField(Key, "invalid-dedupe"), "not-a-ulid");
        Assert.Null(await storage.FindByDeduplicationKeyAsync(Key, "invalid-dedupe"));
    }

    [RedisFact]
    public async Task Cancellation_Should_Be_Idempotent_And_Respect_Locks()
    {
        using var provider = CreateProvider();
        var pending = NewAction();
        var locked = NewAction();
        var completed = NewAction();
        using (var scope = provider.CreateScope())
        {
            var storage = (RedisMuleStorage)scope.ServiceProvider.GetRequiredService<IMuleStorage>();
            await storage.AddRangeAsync([pending, locked, completed]);
            await storage.SaveChangesAsync();
            Assert.NotNull(await storage.LockAsync(locked.Id, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow));
            Assert.NotNull(await storage.LockAsync(completed.Id, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow));
            await storage.MarkCompletedAsync(completed.Id, DateTimeOffset.UtcNow);

            var cancelled = await storage.CancelAsync(pending.Id, DateTimeOffset.UtcNow);
            var repeated = await storage.CancelAsync(pending.Id, DateTimeOffset.UtcNow);
            var tooLate = await storage.CancelAsync(locked.Id, DateTimeOffset.UtcNow);
            var missing = await storage.CancelAsync(Ulid.NewUlid(), DateTimeOffset.UtcNow);

            Assert.Equal(MuleCancellationOutcome.Cancelled, cancelled.Outcome);
            Assert.Equal(MuleCancellationOutcome.AlreadyCancelled, repeated.Outcome);
            Assert.Equal(MuleCancellationOutcome.TooLate, tooLate.Outcome);
            Assert.Equal(MuleCancellationOutcome.NotFound, missing.Outcome);
            Assert.True(await storage.MarkCancelledAsync(locked.Id, DateTimeOffset.UtcNow));
            Assert.True(await storage.MarkCancelledAsync(locked.Id, DateTimeOffset.UtcNow));
            Assert.False(await storage.MarkCancelledAsync(completed.Id, DateTimeOffset.UtcNow));
        }
    }

    [RedisFact]
    public async Task Storage_Should_Handle_Default_Lane_Null_Error_And_Invalid_Metrics()
    {
        using var provider = CreateProvider();
        using var scope = provider.CreateScope();
        var storage = (RedisMuleStorage)scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var action = NewAction();
        action.Lane = " ";
        await storage.AddAsync(action);
        await storage.SaveChangesAsync();

        var claimed = Assert.Single(await storage.ClaimPendingAsync(null, 1, TimeSpan.Zero, DateTimeOffset.UtcNow));
        await storage.MarkFailedAsync(claimed.Id, null, DateTimeOffset.UtcNow, null);

        var database = await scope.ServiceProvider.GetRequiredService<RedisMuleConnection>().GetDatabaseAsync();
        var keys = new RedisMuleKeyspace(_prefix);
        await database.HashSetAsync(keys.BacklogByLane,
        [
            new HashEntry("invalid", "not-a-number"),
            new HashEntry("zero", 0)
        ]);
        await database.HashSetAsync(keys.BacklogByActionKey,
        [
            new HashEntry("invalid", "not-a-number"),
            new HashEntry("zero", 0)
        ]);

        var snapshot = await scope.ServiceProvider.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync();
        Assert.Equal(1, snapshot.Failed);
        Assert.DoesNotContain("invalid", snapshot.BacklogByLane.Keys);
        Assert.Equal(keys.LaneDue(null), keys.LaneDue(MuleSettings.DefaultLane));
    }

    [RedisFact]
    public async Task Replay_Should_Report_All_Scheduling_Outcomes()
    {
        using var provider = CreateProvider();
        using var scope = provider.CreateScope();
        var storage = (RedisMuleStorage)scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var now = DateTimeOffset.UtcNow;
        var missing = await storage.ScheduleAsync(Ulid.NewUlid(), Request(), now);
        var pending = NewAction();
        await storage.AddAsync(pending);
        await storage.SaveChangesAsync();
        var ineligible = await storage.ScheduleAsync(pending.Id, Request(), now);
        await storage.LockAsync(pending.Id, TimeSpan.FromMinutes(1), now);
        await storage.MarkFailedAsync(pending.Id, "failed", now, null);

        var request = Request();
        var scheduled = await storage.ScheduleAsync(pending.Id, request, now);
        var repeated = await storage.ScheduleAsync(pending.Id, request, now);
        var conflict = await storage.ScheduleAsync(
            pending.Id,
            new MuleReplayRequest { RequestId = request.RequestId, RequestedBy = "different" },
            now);
        var existingBranch = await storage.ScheduleAsync(pending.Id, Request(), now);
        var parallel = await storage.ScheduleAsync(
            pending.Id,
            new MuleReplayRequest { RequestId = Ulid.NewUlid(), AllowParallelReplay = true },
            now);

        Assert.Equal(MuleReplayOutcome.NotFound, missing.Outcome);
        Assert.Equal(MuleReplayOutcome.NotEligible, ineligible.Outcome);
        Assert.Equal(MuleReplayOutcome.Scheduled, scheduled.Outcome);
        Assert.Equal(MuleReplayOutcome.AlreadyScheduled, repeated.Outcome);
        Assert.Equal(MuleReplayOutcome.RequestConflict, conflict.Outcome);
        Assert.Equal(MuleReplayOutcome.ReplayAlreadyExists, existingBranch.Outcome);
        Assert.Equal(MuleReplayOutcome.Scheduled, parallel.Outcome);
        Assert.Null(await storage.FindAsync(Ulid.NewUlid()));
        await Assert.ThrowsAsync<ArgumentNullException>(() => storage.ScheduleAsync(pending.Id, null, now));
    }

    [RedisFact]
    public async Task Replay_Lease_Should_Release_And_Materialize_Once()
    {
        using var provider = CreateProvider();
        using var scope = provider.CreateScope();
        var storage = (RedisMuleStorage)scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var source = await CreateFailedAsync(storage);
        var now = DateTimeOffset.UtcNow;
        var scheduled = await storage.ScheduleAsync(source.Id, Request(), now);
        Assert.Equal(now.ToUnixTimeMilliseconds(), (await storage.GetNextScheduledOnUtcAsync(TimeSpan.Zero, now)).Value.ToUnixTimeMilliseconds());

        var claim = Assert.Single(await storage.ClaimScheduledAsync(1, TimeSpan.FromSeconds(1), now));
        var next = now.AddSeconds(2);
        await storage.ReleaseAsync(claim, "retry", next);
        Assert.Empty(await storage.ClaimScheduledAsync(1, TimeSpan.FromSeconds(1), now.AddSeconds(1)));
        var reclaimed = Assert.Single(await storage.ClaimScheduledAsync(1, TimeSpan.FromSeconds(1), next));
        var action = ReplayAction(reclaimed, next);
        var stale = new MuleReplayClaim
        {
            Replay = reclaimed.Replay,
            SourceAction = reclaimed.SourceAction,
            LockToken = Ulid.NewUlid()
        };

        Assert.Equal(MuleReplayMaterializationOutcome.LeaseLost, await storage.MaterializeAsync(stale, action, next));
        Assert.Equal(MuleReplayMaterializationOutcome.Created, await storage.MaterializeAsync(reclaimed, action, next));
        Assert.Equal(MuleReplayMaterializationOutcome.Reconciled, await storage.MaterializeAsync(reclaimed, action, next));
        Assert.Equal(MuleReplayStatus.Materialized, (await storage.FindAsync(scheduled.Replay.RequestId)).Status);
        await Assert.ThrowsAsync<ArgumentNullException>(() => storage.MaterializeAsync(null, action, next));
        await Assert.ThrowsAsync<ArgumentNullException>(() => storage.MaterializeAsync(reclaimed, null, next));
        await Assert.ThrowsAsync<ArgumentNullException>(() => storage.ReleaseAsync(null, "", next));
    }

    [RedisFact]
    public async Task Replay_Should_Handle_Future_Due_Time_Null_Fields_And_Null_Release_Error()
    {
        using var provider = CreateProvider();
        using var scope = provider.CreateScope();
        var storage = (RedisMuleStorage)scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var source = await CreateFailedAsync(storage);
        var now = DateTimeOffset.UtcNow;
        var future = now.AddMinutes(2);
        var scheduled = await storage.ScheduleAsync(
            source.Id,
            new MuleReplayRequest
            {
                RequestId = Ulid.NewUlid(),
                AllowParallelReplay = true,
                ScheduleOnUtc = future
            },
            now);

        Assert.Equal(future.ToUnixTimeMilliseconds(), (await storage.GetNextScheduledOnUtcAsync(TimeSpan.Zero, now)).Value.ToUnixTimeMilliseconds());
        var claim = Assert.Single(await storage.ClaimScheduledAsync(1, TimeSpan.Zero, future));
        await storage.ReleaseAsync(claim, null, future.AddSeconds(1));
        var reclaimed = Assert.Single(await storage.ClaimScheduledAsync(1, TimeSpan.Zero, future.AddSeconds(1)));
        var action = new DurableAction
        {
            Id = scheduled.Replay.ReplayActionId,
            Key = reclaimed.SourceAction.Key,
            Lane = " ",
            CreatedOnUtc = future.AddSeconds(1)
        };

        Assert.Equal(MuleReplayMaterializationOutcome.Created, await storage.MaterializeAsync(reclaimed, action, future.AddSeconds(1)));
    }

    [RedisFact]
    public async Task Scheduled_Replay_Should_Be_Cancellable_By_Reserved_Action_Id()
    {
        using var provider = CreateProvider();
        using var scope = provider.CreateScope();
        var storage = (RedisMuleStorage)scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var source = await CreateFailedAsync(storage);
        var scheduled = await storage.ScheduleAsync(
            source.Id,
            new MuleReplayRequest { RequestId = Ulid.NewUlid(), ScheduleOnUtc = DateTimeOffset.UtcNow.AddHours(1) },
            DateTimeOffset.UtcNow);

        var cancelled = await storage.CancelAsync(scheduled.Replay.ReplayActionId, DateTimeOffset.UtcNow);
        var repeated = await storage.CancelAsync(scheduled.Replay.ReplayActionId, DateTimeOffset.UtcNow);

        Assert.Equal(MuleCancellationOutcome.Cancelled, cancelled.Outcome);
        Assert.Equal(MuleCancellationOutcome.AlreadyCancelled, repeated.Outcome);
        Assert.Equal(MuleReplayStatus.Cancelled, (await storage.FindAsync(scheduled.Replay.RequestId)).Status);
    }

    [RedisFact]
    public async Task Connection_Should_Use_Configuration_And_Factory()
    {
        var connectionString = Environment.GetEnvironmentVariable(RedisMuleTests.ConnectionStringEnvironmentVariable);
        var configuration = ConfigurationOptions.Parse(connectionString);
        configuration.AbortOnConnectFail = false;
        using (var configured = CreateProvider(options => options.ConfigurationOptions = configuration))
        using (var scope = configured.CreateScope())
            Assert.NotNull(await scope.ServiceProvider.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync());

        await using var shared = await ConnectionMultiplexer.ConnectAsync(connectionString);
        var calls = 0;
        using (var factory = CreateProvider(options =>
               {
                   options.ConnectionFactory = (_, _) =>
                   {
                       Interlocked.Increment(ref calls);
                       return ValueTask.FromResult<IConnectionMultiplexer>(shared);
                   };
                   options.DisposeConnection = false;
               }))
        {
            using var scope = factory.CreateScope();
            await scope.ServiceProvider.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync();
        }

        Assert.Equal(1, calls);
        Assert.True(shared.IsConnected);
    }

    [RedisFact]
    public async Task Connection_Should_Use_Localhost_Default_When_Connection_String_Is_Blank()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMule(mule => mule.UseRedis(options =>
        {
            options.ConnectionString = " ";
            options.KeyPrefix = _prefix;
        }));
        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();

        Assert.NotNull(await scope.ServiceProvider.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync());
        var connection = scope.ServiceProvider.GetRequiredService<RedisMuleConnection>();
        var database = await connection.GetDatabaseAsync();
        await database.KeyDeleteAsync(connection.Keyspace.Schema);
    }

    [RedisFact]
    public async Task Invalid_Schema_And_Null_Factory_Result_Should_Fail_Fast()
    {
        var connectionString = Environment.GetEnvironmentVariable(RedisMuleTests.ConnectionStringEnvironmentVariable);
        var keys = new RedisMuleKeyspace(_prefix);
        await using (var raw = await ConnectionMultiplexer.ConnectAsync(connectionString))
            await raw.GetDatabase().StringSetAsync(keys.Schema, "999");

        using (var invalidSchema = CreateProvider())
        using (var scope = invalidSchema.CreateScope())
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                scope.ServiceProvider.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync());

        await CleanupAsync();
        using var nullFactory = CreateProvider(options =>
            options.ConnectionFactory = (_, _) => ValueTask.FromResult<IConnectionMultiplexer>(null));
        using var nullScope = nullFactory.CreateScope();
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            nullScope.ServiceProvider.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync());
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public Task DisposeAsync() => CleanupAsync();

    private ServiceProvider CreateProvider(Action<RedisMuleOptions> configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMule(mule => mule
            .UseRedis(options =>
            {
                options.ConnectionString = Environment.GetEnvironmentVariable(RedisMuleTests.ConnectionStringEnvironmentVariable);
                options.KeyPrefix = _prefix;
                options.WriteBatchSize = 2;
                configure?.Invoke(options);
            })
            .UseCleanup(MuleCleanupMode.Disabled));
        return services.BuildServiceProvider();
    }

    private async Task CleanupAsync()
    {
        var connectionString = Environment.GetEnvironmentVariable(RedisMuleTests.ConnectionStringEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
            return;
        await using var connection = await ConnectionMultiplexer.ConnectAsync(connectionString);
        var database = connection.GetDatabase();
        var keys = (RedisResult[])await database.ExecuteAsync("KEYS", new RedisMuleKeyspace(_prefix).Root + "*");
        var redisKeys = keys.Select(x => (RedisKey)((RedisValue)x).ToString()).ToArray();
        if (redisKeys.Length > 0)
            await database.KeyDeleteAsync(redisKeys);
    }

    private static DurableAction NewAction()
        => new()
        {
            Key = Key,
            Payload = "{}",
            PayloadType = typeof(object).AssemblyQualifiedName,
            CreatedOnUtc = DateTimeOffset.UtcNow
        };

    private static async Task<DurableAction> CreateFailedAsync(RedisMuleStorage storage)
    {
        var action = NewAction();
        await storage.AddAsync(action);
        await storage.SaveChangesAsync();
        await storage.LockAsync(action.Id, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow);
        await storage.MarkFailedAsync(action.Id, "failed", DateTimeOffset.UtcNow, null);
        action.Status = DurableActionStatus.Failed;
        return action;
    }

    private static MuleReplayRequest Request()
        => new() { RequestId = Ulid.NewUlid(), RequestedBy = "coverage", Reason = "test" };

    private static DurableAction ReplayAction(MuleReplayClaim claim, DateTimeOffset now)
        => new()
        {
            Id = claim.Replay.ReplayActionId,
            Key = claim.SourceAction.Key,
            Lane = claim.SourceAction.Lane,
            Payload = claim.SourceAction.Payload,
            PayloadType = claim.SourceAction.PayloadType,
            DeduplicationKey = "mule:replay:" + claim.Replay.RequestId,
            CreatedOnUtc = now
        };

    private static HashEntry[] ActionHash(DurableAction action, RedisMuleKeyspace keys)
        =>
        [
            new("id", action.Id.ToString()), new("ct", action.ConcurrencyToken.ToString()),
            new("key", action.Key.Value), new("lane", action.Lane), new("laneDue", keys.LaneDue(action.Lane)),
            new("payload", action.Payload), new("payloadType", action.PayloadType),
            new("metadata", action.Metadata ?? ""), new("correlation", action.CorrelationId ?? ""),
            new("dedupe", action.DeduplicationKey ?? ""), new("status", (int)action.Status),
            new("attempts", action.Attempts), new("error", action.LastError ?? ""),
            new("created", action.CreatedOnUtc.ToUnixTimeMilliseconds()),
            new("locked", RedisMuleActionCodec.Milliseconds(action.LockedOnUtc)),
            new("started", RedisMuleActionCodec.Milliseconds(action.StartedOnUtc)),
            new("next", RedisMuleActionCodec.Milliseconds(action.NextAttemptOnUtc)),
            new("completed", RedisMuleActionCodec.Milliseconds(action.CompletedOnUtc)),
            new("cancelled", RedisMuleActionCodec.Milliseconds(action.CancelledOnUtc)),
            new("terminal", RedisMuleActionCodec.Milliseconds(action.TerminalOnUtc))
        ];

    private static HashEntry[] ReplayHash(Ulid id, DateTimeOffset now, string metadata, string lockToken)
        =>
        [
            new("id", id.ToString()), new("ct", Ulid.NewUlid().ToString()),
            new("source", Ulid.NewUlid().ToString()), new("root", Ulid.NewUlid().ToString()),
            new("action", Ulid.NewUlid().ToString()), new("sequence", 2),
            new("branch", Ulid.NewUlid().ToString()), new("key", Key.Value),
            new("lane", "critical"), new("correlation", "correlation"),
            new("originalDedupe", "dedupe"), new("status", (int)MuleReplayStatus.Materialized),
            new("requestedBy", "coverage"), new("reason", "test"), new("metadata", metadata),
            new("requested", now.ToUnixTimeMilliseconds()), new("schedule", now.ToUnixTimeMilliseconds()),
            new("attempts", 1), new("next", now.ToUnixTimeMilliseconds()), new("error", "error"),
            new("locked", now.ToUnixTimeMilliseconds()), new("lockToken", lockToken),
            new("materialized", now.ToUnixTimeMilliseconds()), new("cancelled", now.ToUnixTimeMilliseconds())
        ];
}
