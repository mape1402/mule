namespace Mule.Redis.Tests;

using System.Collections.Concurrent;
using System.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule.Diagnostics;
using StackExchange.Redis;

[Collection("Redis durable storage")]
public sealed class RedisMuleTests : IAsyncLifetime
{
    public const string ConnectionStringEnvironmentVariable = "MULE_REDIS_CONNECTION_STRING";

    private static readonly ActionKey CaptureKey = ActionKey.From("tests.redis.capture.v1");
    private static readonly ActionKey ResilientKey = ActionKey.From("tests.redis.resilient.v1");
    private readonly string _prefix = $"mule-redis-tests-{Guid.NewGuid():N}";

    [RedisFact]
    public async Task Redis_Should_Be_The_Authoritative_Storage_For_Execution()
    {
        using var host = CreateHost();
        await host.StartAsync();

        IReadOnlyCollection<Ulid> ids;
        using (var scope = host.Services.CreateScope())
        {
            ids = await scope.ServiceProvider.GetRequiredService<IMuleClient>()
                .EnqueueManyAsync(Enumerable.Range(0, 25)
                    .Select(index => MuleIntent.For(CaptureKey, new TestPayload($"value-{index}"))));
        }

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitForCountAsync(25);
        using (var scope = host.Services.CreateScope())
        {
            var snapshot = await scope.ServiceProvider.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync();
            Assert.Equal(25, snapshot.Completed);
            Assert.Equal(0, snapshot.Pending);
        }

        await host.StopAsync();
        Assert.Equal(25, ids.Distinct().Count());
        Assert.Equal(25, probe.Values.Distinct().Count());
    }

    [RedisFact]
    public async Task Pending_Actions_Should_Survive_Provider_Recreation()
    {
        Ulid id;
        using (var provider = CreateServices().BuildServiceProvider())
        using (var scope = provider.CreateScope())
        {
            id = await scope.ServiceProvider.GetRequiredService<IMuleClient>()
                .EnqueueAsync(CaptureKey, new TestPayload("restart"));
        }

        using var host = CreateHost();
        await host.StartAsync();
        await host.Services.GetRequiredService<TestProbe>().WaitForCountAsync(1);
        await host.StopAsync();

        using var verification = CreateServices().BuildServiceProvider();
        using var verificationScope = verification.CreateScope();
        var snapshot = await verificationScope.ServiceProvider.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync();
        Assert.Equal(1, snapshot.Completed);
        Assert.NotEqual(Ulid.Empty, id);
    }

    [RedisFact]
    public async Task Concurrent_Producers_Should_Deduplicate_Atomically()
    {
        using var provider = CreateServices().BuildServiceProvider();
        var tasks = Enumerable.Range(0, 32).Select(async _ =>
        {
            using var scope = provider.CreateScope();
            return await scope.ServiceProvider.GetRequiredService<IMuleClient>().EnqueueAsync(
                CaptureKey,
                new TestPayload("duplicate"),
                options => options.DeduplicationKey = "same-operation");
        });

        var ids = await Task.WhenAll(tasks);
        using var diagnosticsScope = provider.CreateScope();
        var snapshot = await diagnosticsScope.ServiceProvider.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync();
        Assert.Single(ids.Distinct());
        Assert.Equal(1, snapshot.Pending);
        Assert.True(snapshot.DuplicatesIgnored > 0);
    }

    [RedisFact]
    public async Task Replica_Claimers_Should_Receive_Disjoint_Batches()
    {
        using var provider = CreateServices().BuildServiceProvider();
        using (var scope = provider.CreateScope())
        {
            var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
            await client.EnqueueManyAsync(Enumerable.Range(0, 100)
                .Select(index => MuleIntent.For(CaptureKey, new TestPayload(index.ToString()))));
        }

        var now = DateTimeOffset.UtcNow;
        var claims = await Task.WhenAll(Enumerable.Range(0, 4).Select(async _ =>
        {
            using var scope = provider.CreateScope();
            return await scope.ServiceProvider.GetRequiredService<IMuleStorage>()
                .ClaimPendingAsync(MuleSettings.DefaultLane, 25, TimeSpan.FromMinutes(1), now);
        }));
        var actions = claims.SelectMany(x => x).ToArray();

        Assert.Equal(100, actions.Length);
        Assert.Equal(100, actions.Select(x => x.Id).Distinct().Count());
        Assert.All(actions, action => Assert.Equal(DurableActionStatus.Locked, action.Status));
    }

    [RedisFact]
    public async Task Thousand_Action_Batch_Should_Be_Claimed_And_Completed_Without_Loss()
    {
        const int count = 1_000;
        using var provider = CreateServices().BuildServiceProvider();
        using (var enqueueScope = provider.CreateScope())
        {
            var ids = await enqueueScope.ServiceProvider.GetRequiredService<IMuleClient>()
                .EnqueueManyAsync(Enumerable.Range(0, count)
                    .Select(index => MuleIntent.For(CaptureKey, new TestPayload($"batch-{index}"))));
            Assert.Equal(count, ids.Distinct().Count());
        }

        var now = DateTimeOffset.UtcNow;
        var claims = await Task.WhenAll(Enumerable.Range(0, 4).Select(async _ =>
        {
            using var claimScope = provider.CreateScope();
            return await claimScope.ServiceProvider.GetRequiredService<IMuleStorage>()
                .ClaimPendingAsync(MuleSettings.DefaultLane, count / 4, TimeSpan.FromMinutes(1), now);
        }));
        var actions = claims.SelectMany(x => x).ToArray();
        Assert.Equal(count, actions.Length);
        Assert.Equal(count, actions.Select(x => x.Id).Distinct().Count());

        using (var completionScope = provider.CreateScope())
        {
            await completionScope.ServiceProvider.GetRequiredService<IMuleBatchTerminalStorage>()
                .MarkCompletedRangeAsync(actions.Select(action => new MuleCompletedAction(action.Id, now)).ToArray());
        }

        using var diagnosticsScope = provider.CreateScope();
        var snapshot = await diagnosticsScope.ServiceProvider.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync();
        Assert.Equal(count, snapshot.Completed);
        Assert.Equal(0, snapshot.Pending);
        Assert.Equal(0, snapshot.Locked);
    }

    [RedisFact]
    public async Task Expired_Lock_Should_Be_Claimable_Again()
    {
        using var provider = CreateServices().BuildServiceProvider();
        Ulid id;
        using (var scope = provider.CreateScope())
            id = await scope.ServiceProvider.GetRequiredService<IMuleClient>().EnqueueAsync(CaptureKey, new TestPayload("lease"));

        using var firstScope = provider.CreateScope();
        var first = await firstScope.ServiceProvider.GetRequiredService<IMuleStorage>()
            .ClaimPendingAsync(MuleSettings.DefaultLane, 1, TimeSpan.FromSeconds(1), DateTimeOffset.UtcNow);
        using var secondScope = provider.CreateScope();
        var second = await secondScope.ServiceProvider.GetRequiredService<IMuleStorage>()
            .ClaimPendingAsync(MuleSettings.DefaultLane, 1, TimeSpan.FromSeconds(1), DateTimeOffset.UtcNow.AddSeconds(2));

        Assert.Equal(id, Assert.Single(first).Id);
        Assert.Equal(id, Assert.Single(second).Id);
    }

    [RedisFact]
    public async Task Cancellation_And_Claim_Should_Have_One_Winner()
    {
        using var provider = CreateServices().BuildServiceProvider();
        Ulid id;
        using (var scope = provider.CreateScope())
            id = await scope.ServiceProvider.GetRequiredService<IMuleClient>().EnqueueAsync(CaptureKey, new TestPayload("race"));

        var now = DateTimeOffset.UtcNow;
        using var cancellationScope = provider.CreateScope();
        using var claimScope = provider.CreateScope();
        var cancelTask = cancellationScope.ServiceProvider.GetRequiredService<IMuleCancellationService>().CancelAsync(id).AsTask();
        var claimTask = claimScope.ServiceProvider.GetRequiredService<IMuleStorage>()
            .ClaimPendingAsync(MuleSettings.DefaultLane, 1, TimeSpan.FromMinutes(1), now);
        await Task.WhenAll(cancelTask, claimTask);

        var cancelled = cancelTask.Result.Outcome == MuleCancellationOutcome.Cancelled;
        Assert.Equal(cancelled, claimTask.Result.Count == 0);
        Assert.Contains(cancelTask.Result.Outcome, new[] { MuleCancellationOutcome.Cancelled, MuleCancellationOutcome.TooLate });
    }

    [RedisFact]
    public async Task Ambient_Transaction_Should_Be_Rejected_By_Default()
    {
        using var provider = CreateServices().BuildServiceProvider();
        using var scope = provider.CreateScope();
        using var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await scope.ServiceProvider.GetRequiredService<IMuleClient>()
                .EnqueueAsync(CaptureKey, new TestPayload("transaction")));

        Assert.Contains("cannot participate atomically", exception.Message);
    }

    [RedisFact]
    public async Task Failed_Action_Should_Be_Retried_And_Completed()
    {
        using var host = CreateHost();
        var probe = host.Services.GetRequiredService<ResilientProbe>();
        probe.Fail("retry", 1);
        await host.StartAsync();

        using (var scope = host.Services.CreateScope())
            await scope.ServiceProvider.GetRequiredService<IMuleClient>()
                .EnqueueAsync(ResilientKey, new TestPayload("retry"));

        await probe.WaitForSuccessAsync("retry");
        using (var scope = host.Services.CreateScope())
        {
            var snapshot = await scope.ServiceProvider.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync();
            Assert.Equal(1, snapshot.Completed);
            Assert.Equal(1, snapshot.RetriesByActionKey[ResilientKey]);
        }

        await host.StopAsync();
        Assert.Equal(2, probe.Attempts("retry"));
    }

    [RedisFact]
    public async Task Replay_Should_Be_Scheduled_Materialized_And_Executed_Atomically()
    {
        using var host = CreateHost();
        var probe = host.Services.GetRequiredService<ResilientProbe>();
        probe.Fail("replay", 10);
        await host.StartAsync();

        Ulid sourceId;
        using (var scope = host.Services.CreateScope())
            sourceId = await scope.ServiceProvider.GetRequiredService<IMuleClient>()
                .EnqueueAsync(ResilientKey, new TestPayload("replay"));
        await WaitForSnapshotAsync(host, snapshot => snapshot.Failed == 1);

        probe.Fail("replay", 0);
        var requestId = Ulid.NewUlid();
        MuleReplayResult scheduled;
        using (var scope = host.Services.CreateScope())
        {
            scheduled = await scope.ServiceProvider.GetRequiredService<IMuleReplayService>().ReplayAsync(
                sourceId,
                new MuleReplayRequest
                {
                    RequestId = requestId,
                    RequestedBy = "integration-test",
                    Reason = "dependency recovered",
                    Metadata = new Dictionary<string, string> { ["ticket"] = "INC-42" }
                });
        }

        await probe.WaitForSuccessAsync("replay");
        await WaitForReplayAsync(host, requestId, MuleReplayStatus.Materialized);
        using (var scope = host.Services.CreateScope())
        {
            var replayService = scope.ServiceProvider.GetRequiredService<IMuleReplayService>();
            var repeated = await replayService.ReplayAsync(
                sourceId,
                new MuleReplayRequest
                {
                    RequestId = requestId,
                    RequestedBy = "integration-test",
                    Reason = "dependency recovered",
                    Metadata = new Dictionary<string, string> { ["ticket"] = "INC-42" }
                });
            var info = await replayService.GetReplayAsync(requestId);
            Assert.Equal(MuleReplayOutcome.AlreadyScheduled, repeated.Outcome);
            Assert.Equal(MuleReplayStatus.Materialized, info.Status);
            Assert.Equal("INC-42", info.Metadata["ticket"]);
        }

        await host.StopAsync();
        Assert.Equal(MuleReplayOutcome.Scheduled, scheduled.Outcome);
        Assert.NotEqual(sourceId, scheduled.Replay.ReplayActionId);
    }

    [RedisFact]
    public async Task Cleanup_Should_Delete_Completed_Action_And_Release_Deduplication_Key()
    {
        using var provider = CreateServices().BuildServiceProvider();
        Ulid firstId;
        using (var scope = provider.CreateScope())
        {
            firstId = await scope.ServiceProvider.GetRequiredService<IMuleClient>().EnqueueAsync(
                CaptureKey,
                new TestPayload("cleanup"),
                options => options.DeduplicationKey = "cleanup-key");
            var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
            Assert.NotNull(await storage.LockAsync(firstId, TimeSpan.FromMinutes(1), DateTimeOffset.UtcNow));
            await storage.MarkCompletedAsync(firstId, DateTimeOffset.UtcNow.AddMinutes(-2));
            await storage.SaveChangesAsync();
        }

        using (var scope = provider.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
            Assert.NotNull(await storage.GetOldestCompletedOnUtcAsync());
            Assert.Equal(1, await storage.CleanCompletedAsync(DateTimeOffset.UtcNow.AddMinutes(-1), 10));
            var secondId = await scope.ServiceProvider.GetRequiredService<IMuleClient>().EnqueueAsync(
                CaptureKey,
                new TestPayload("cleanup-again"),
                options => options.DeduplicationKey = "cleanup-key");
            Assert.NotEqual(firstId, secondId);
        }
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync() => await CleanupAsync(_prefix);

    private IHost CreateHost()
        => Host.CreateDefaultBuilder()
            .ConfigureServices(services => ConfigureServices(services, includeAction: true))
            .Build();

    private ServiceCollection CreateServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        ConfigureServices(services, includeAction: false);
        return services;
    }

    private void ConfigureServices(IServiceCollection services, bool includeAction)
    {
        services.AddSingleton<TestProbe>();
        services.AddSingleton<ResilientProbe>();
        services.AddMule(mule =>
        {
            mule.UseRedis(options =>
                {
                    options.ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);
                    options.KeyPrefix = _prefix;
                    options.WriteBatchSize = 100;
                })
                .UseRecovery(MuleRecoveryMode.Scheduled)
                .UseCleanup(MuleCleanupMode.Disabled)
                .Configure(settings =>
                {
                    settings.ImmediateDispatch = true;
                    settings.DispatchInterval = TimeSpan.FromMilliseconds(25);
                    settings.DispatchBatchSize = 100;
                    settings.WorkerCount = 2;
                    settings.MaxDegreeOfParallelism = 16;
                    settings.LockTimeout = TimeSpan.FromSeconds(5);
                    settings.MaxAttempts = 2;
                    settings.RetryDelay = TimeSpan.FromMilliseconds(25);
                    settings.Replay.PollingInterval = TimeSpan.FromMilliseconds(25);
                    settings.Replay.RetryDelay = TimeSpan.FromMilliseconds(25);
                });
            if (includeAction)
            {
                mule.For<CaptureAction, TestPayload>(CaptureKey);
                mule.For<ResilientAction, TestPayload>(ResilientKey);
            }
        });
    }

    private static async Task WaitForSnapshotAsync(IHost host, Func<MuleDiagnosticsSnapshot, bool> predicate)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        while (DateTimeOffset.UtcNow < deadline)
        {
            using var scope = host.Services.CreateScope();
            var snapshot = await scope.ServiceProvider.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync();
            if (predicate(snapshot))
                return;
            await Task.Delay(20);
        }
        throw new TimeoutException("Timed out waiting for the expected Redis Mule diagnostic state.");
    }

    private static async Task WaitForReplayAsync(IHost host, Ulid requestId, MuleReplayStatus status)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        while (DateTimeOffset.UtcNow < deadline)
        {
            using var scope = host.Services.CreateScope();
            var replay = await scope.ServiceProvider.GetRequiredService<IMuleReplayService>().GetReplayAsync(requestId);
            if (replay?.Status == status)
                return;
            await Task.Delay(20);
        }
        throw new TimeoutException($"Timed out waiting for replay {requestId} to reach {status}.");
    }

    private static async Task CleanupAsync(string prefix)
    {
        var connectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
            return;

        await using var connection = await ConnectionMultiplexer.ConnectAsync(connectionString);
        var keyspace = new RedisMuleKeyspace(prefix);
        var database = connection.GetDatabase();
        var result = (RedisResult[])await database.ExecuteAsync("KEYS", keyspace.Root + "*");
        var keys = result.Select(x => (RedisKey)((RedisValue)x).ToString()).ToArray();
        if (keys.Length > 0)
            await database.KeyDeleteAsync(keys);
    }

    public sealed record TestPayload(string Value);

    public sealed class CaptureAction(TestProbe probe) : IMuleAction<TestPayload>
    {
        public ValueTask ExecuteAsync(MuleActionContext<TestPayload> context, CancellationToken cancellationToken)
        {
            probe.Record(context.Payload.Value);
            return ValueTask.CompletedTask;
        }
    }

    public sealed class ResilientAction(ResilientProbe probe) : IMuleAction<TestPayload>
    {
        public ValueTask ExecuteAsync(MuleActionContext<TestPayload> context, CancellationToken cancellationToken)
        {
            probe.Execute(context.Payload.Value);
            return ValueTask.CompletedTask;
        }
    }

    public sealed class ResilientProbe
    {
        private readonly ConcurrentDictionary<string, int> _remainingFailures = new();
        private readonly ConcurrentDictionary<string, int> _attempts = new();
        private readonly ConcurrentDictionary<string, TaskCompletionSource> _successes = new();

        public void Fail(string value, int times) => _remainingFailures[value] = times;

        public int Attempts(string value) => _attempts.GetValueOrDefault(value);

        public void Execute(string value)
        {
            _attempts.AddOrUpdate(value, 1, (_, count) => count + 1);
            while (_remainingFailures.TryGetValue(value, out var remaining) && remaining > 0)
            {
                if (_remainingFailures.TryUpdate(value, remaining - 1, remaining))
                    throw new InvalidOperationException("Planned Redis Mule failure.");
            }

            _successes.GetOrAdd(value, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))
                .TrySetResult();
        }

        public async Task WaitForSuccessAsync(string value)
        {
            var completion = _successes.GetOrAdd(
                value,
                _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            await completion.Task.WaitAsync(TimeSpan.FromSeconds(30));
        }
    }

    public sealed class TestProbe
    {
        private readonly ConcurrentQueue<string> _values = new();

        public IReadOnlyCollection<string> Values => _values.ToArray();

        public void Record(string value) => _values.Enqueue(value);

        public async Task WaitForCountAsync(int count)
        {
            var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
            while (DateTimeOffset.UtcNow < deadline)
            {
                if (_values.Count >= count)
                    return;
                await Task.Delay(20);
            }
            throw new TimeoutException($"Timed out waiting for {count} Redis Mule actions. Received {_values.Count}.");
        }
    }
}
