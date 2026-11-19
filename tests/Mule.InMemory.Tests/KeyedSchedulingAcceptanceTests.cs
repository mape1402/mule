namespace Mule.InMemory.Tests;

using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

public sealed class KeyedSchedulingAcceptanceTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.keyed-scheduling.v1");

    [Fact]
    public async Task Busy_Key_Should_Not_Occupy_The_Executor_Needed_By_Another_Key()
    {
        var probe = new SchedulingProbe();
        using var host = CreateHost(probe);
        await host.StartAsync();

        try
        {
            await EnqueueAsync(host, new TestPayload("a-1", Block: true), "instance:a");
            await probe.BlockingStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

            await EnqueueAsync(host, new TestPayload("a-2"), "instance:a");
            await EnqueueAsync(host, new TestPayload("b"), "instance:b");

            await probe.Completed("b").WaitAsync(TimeSpan.FromSeconds(2));
            Assert.DoesNotContain("a-2", probe.Started);

            probe.ReleaseBlocking.TrySetResult();
            await probe.Completed("a-2").WaitAsync(TimeSpan.FromSeconds(2));
        }
        finally
        {
            probe.ReleaseBlocking.TrySetResult();
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task Hot_Key_Should_Not_Starve_Fifty_Independent_Keys()
    {
        var probe = new SchedulingProbe();
        using var host = CreateHost(probe, settings =>
        {
            settings.ImmediateDispatch = false;
            settings.DispatchBatchSize = 4;
            settings.ExecutionKeyCandidateBatchSize = 8;
        });

        await EnqueueAsync(host, new TestPayload("hot-0", Block: true), "instance:hot");
        for (var i = 1; i <= 100; i++)
            await EnqueueAsync(host, new TestPayload($"hot-{i}"), "instance:hot");
        for (var i = 0; i < 50; i++)
            await EnqueueAsync(host, new TestPayload($"independent-{i}"), $"instance:{i}");

        await host.StartAsync();
        try
        {
            await probe.BlockingStarted.Task.WaitAsync(TimeSpan.FromSeconds(3));
            var independent = Enumerable.Range(0, 50)
                .Select(i => probe.Completed($"independent-{i}"));

            await Task.WhenAll(independent).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.DoesNotContain("hot-1", probe.Started);
        }
        finally
        {
            probe.ReleaseBlocking.TrySetResult();
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task Execution_Key_Should_Be_Shared_Across_Lanes()
    {
        var probe = new SchedulingProbe();
        using var host = CreateHost(probe, settings =>
        {
            settings.Lanes["callback"] = new MuleLaneSettings
            {
                WorkerCount = 1,
                MaxDegreeOfParallelism = 1
            };
            settings.Lanes["timeout"] = new MuleLaneSettings
            {
                WorkerCount = 1,
                MaxDegreeOfParallelism = 2
            };
        });
        await host.StartAsync();

        try
        {
            await EnqueueAsync(host, new TestPayload("a-callback", Block: true), "instance:a", "callback");
            await probe.BlockingStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

            await EnqueueAsync(host, new TestPayload("a-timeout"), "instance:a", "timeout");
            await EnqueueAsync(host, new TestPayload("b-timeout"), "instance:b", "timeout");

            await probe.Completed("b-timeout").WaitAsync(TimeSpan.FromSeconds(2));
            Assert.DoesNotContain("a-timeout", probe.Started);

            probe.ReleaseBlocking.TrySetResult();
            await probe.Completed("a-timeout").WaitAsync(TimeSpan.FromSeconds(2));
        }
        finally
        {
            probe.ReleaseBlocking.TrySetResult();
            await host.StopAsync();
        }
    }

    [Fact]
    public async Task Deferred_Action_Should_Keep_Its_Identity_And_Failure_Budget()
    {
        var probe = new SchedulingProbe();
        using var host = CreateHost(probe);
        await host.StartAsync();

        var actionId = await EnqueueAsync(
            host,
            new TestPayload("deferred", Defer: true),
            "instance:a",
            deduplicationKey: "deferred-1");
        await EnqueueAsync(host, new TestPayload("b"), "instance:b");

        await probe.Deferred.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await probe.Completed("b").WaitAsync(TimeSpan.FromSeconds(2));
        var action = await WaitForActionAsync(
            host,
            actionId,
            x => x.Status == DurableActionStatus.Pending && x.PendingReason == DurableActionPendingReason.Deferred);

        Assert.Equal(actionId, action.Id);
        Assert.Equal("deferred-1", action.DeduplicationKey);
        Assert.Equal("instance:a", action.CorrelationId);
        Assert.Equal(0, action.Attempts);
        Assert.Equal("external-lease-busy", action.DeferralReasonCode);
        Assert.NotNull(action.NextAttemptOnUtc);
        Assert.DoesNotContain("deferred", probe.CompletedValues);

        await host.StopAsync();
    }

    [Fact]
    public async Task Exception_After_Deferral_Request_Should_Follow_The_Failure_Path()
    {
        var probe = new SchedulingProbe();
        using var host = CreateHost(probe, settings => settings.MaxAttempts = 1);
        await host.StartAsync();

        var actionId = await EnqueueAsync(
            host,
            new TestPayload("defer-then-fail", Defer: true, FailAfterDefer: true),
            "instance:a");
        var action = await WaitForActionAsync(host, actionId, x => x.Status == DurableActionStatus.Failed);

        Assert.Equal(1, action.Attempts);
        Assert.Equal(DurableActionPendingReason.None, action.PendingReason);
        Assert.Null(action.DeferralReasonCode);
        Assert.Contains("failure after deferral", action.LastError);

        await host.StopAsync();
    }

    [Fact]
    public async Task Expired_Claim_Should_Not_Complete_A_Newer_Claim()
    {
        using var host = CreateHost(new SchedulingProbe());
        using var scope = host.Services.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var executionStorage = Assert.IsAssignableFrom<IMuleExecutionStorage>(storage);
        var now = DateTimeOffset.UtcNow;
        var action = new DurableAction
        {
            Key = Key,
            Lane = MuleSettings.DefaultLane,
            Payload = "{}",
            PayloadType = typeof(TestPayload).AssemblyQualifiedName,
            Status = DurableActionStatus.Pending,
            CreatedOnUtc = now
        };
        await storage.AddAsync(action);
        await storage.SaveChangesAsync();

        var first = await storage.LockAsync(action.Id, TimeSpan.FromMinutes(1), now);
        var second = await storage.LockAsync(action.Id, TimeSpan.FromMinutes(1), now.AddMinutes(2));

        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.NotEqual(first.ConcurrencyToken, second.ConcurrencyToken);
        Assert.Equal(
            MuleActionTransitionOutcome.LeaseLost,
            await executionStorage.CompleteClaimAsync(action.Id, first.ConcurrencyToken, now.AddMinutes(2)));
        Assert.Equal(
            MuleActionTransitionOutcome.Applied,
            await executionStorage.CompleteClaimAsync(action.Id, second.ConcurrencyToken, now.AddMinutes(2)));
    }

    private static IHost CreateHost(SchedulingProbe probe, Action<MuleSettings> configure = null)
        => Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(probe);
                services.AddMule(mule => mule
                    .UseInMemory()
                    .Configure(settings =>
                    {
                        settings.RecoveryMode = MuleRecoveryMode.Scheduled;
                        settings.DispatchInterval = TimeSpan.FromHours(1);
                        settings.WorkerCount = 2;
                        settings.MaxDegreeOfParallelism = 2;
                        settings.DispatchBatchSize = 16;
                        settings.ExecutionKeyContentionDelay = TimeSpan.FromMilliseconds(10);
                        settings.MinimumDeferralDelay = TimeSpan.FromMilliseconds(10);
                        settings.ExecutionKeySelector = action => string.IsNullOrWhiteSpace(action.CorrelationId)
                            ? null
                            : ExecutionKey.From(action.CorrelationId);
                        configure?.Invoke(settings);
                    })
                    .For<SchedulingProbe, TestPayload>(Key, static (service, context, token) =>
                        service.ExecuteAsync(context, token)));
            })
            .Build();

    private static async Task<Ulid> EnqueueAsync(
        IHost host,
        TestPayload payload,
        string correlationId,
        string lane = null,
        string deduplicationKey = null)
    {
        using var scope = host.Services.CreateScope();
        return await scope.ServiceProvider.GetRequiredService<IMuleClient>().EnqueueAsync(
            Key,
            payload,
            options =>
            {
                options.CorrelationId = correlationId;
                options.Lane = lane ?? MuleSettings.DefaultLane;
                options.DeduplicationKey = deduplicationKey;
            });
    }

    private static async Task<DurableAction> WaitForActionAsync(
        IHost host,
        Ulid actionId,
        Func<DurableAction, bool> predicate)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var mule = host.Services.GetRequiredService<IInMemoryMule>();
        while (!timeout.IsCancellationRequested)
        {
            var action = mule.Actions.Single(x => x.Id == actionId);
            if (predicate(action))
                return action;

            await Task.Delay(10, timeout.Token);
        }

        throw new TimeoutException($"Action '{actionId}' did not reach the expected state.");
    }

    private sealed record TestPayload(
        string Value,
        bool Block = false,
        bool Defer = false,
        bool FailAfterDefer = false);

    private sealed class SchedulingProbe
    {
        private readonly ConcurrentDictionary<string, TaskCompletionSource> _completed = new(StringComparer.Ordinal);

        public TaskCompletionSource BlockingStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource ReleaseBlocking { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Deferred { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ConcurrentBag<string> Started { get; } = new();

        public ConcurrentBag<string> CompletedValues { get; } = new();

        public Task Completed(string value)
            => _completed.GetOrAdd(value, static _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).Task;

        public async ValueTask ExecuteAsync(
            MuleActionContext<TestPayload> context,
            CancellationToken cancellationToken)
        {
            Started.Add(context.Payload.Value);
            if (context.Payload.Block)
            {
                BlockingStarted.TrySetResult();
                await ReleaseBlocking.Task.WaitAsync(cancellationToken);
            }

            if (context.Payload.Defer)
            {
                context.DeferUntil(
                    DateTimeOffset.UtcNow.AddSeconds(30),
                    DeferralReason.From("external-lease-busy"));
                Deferred.TrySetResult();
                if (context.Payload.FailAfterDefer)
                    throw new InvalidOperationException("failure after deferral");
                return;
            }

            CompletedValues.Add(context.Payload.Value);
            _completed
                .GetOrAdd(context.Payload.Value, static _ => new(TaskCreationOptions.RunContinuationsAsynchronously))
                .TrySetResult();
        }
    }
}
