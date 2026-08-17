namespace Mule.InMemory.Tests;

using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule.Diagnostics;

public sealed class InMemoryMuleIntegrationTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.capture.v1");

    [Fact]
    public async Task EnqueueAsync_Should_Store_Action()
    {
        using var host = CreateHost();
        var client = host.Services.GetRequiredService<IServiceScopeFactory>().CreateScope().ServiceProvider.GetRequiredService<IMuleClient>();

        await client.EnqueueAsync(Key, new TestPayload("stored"));

        var mule = host.Services.GetRequiredService<IInMemoryMule>();
        var action = Assert.Single(mule.Actions);

        Assert.Equal(Key, action.Key);
        Assert.Equal(DurableActionStatus.Pending, action.Status);
    }

    [Fact]
    public async Task EnqueueManyAsync_Should_Store_Actions()
    {
        using var host = CreateHost();
        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();

        var ids = await client.EnqueueManyAsync(Enumerable.Range(0, 10)
            .Select(index => MuleIntent.For(Key, new TestPayload($"stored-{index}"))));

        var mule = host.Services.GetRequiredService<IInMemoryMule>();

        Assert.Equal(10, ids.Count);
        Assert.Equal(10, ids.Distinct().Count());
        Assert.Equal(10, mule.Actions.Count);
        Assert.All(mule.Actions, action => Assert.Equal(DurableActionStatus.Pending, action.Status));
    }

    [Fact]
    public async Task HostedService_Should_Execute_Queued_Action()
    {
        using var host = CreateHost();
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
        await client.EnqueueAsync(Key, new TestPayload("hello"));

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitAsync();
        await host.StopAsync();

        Assert.Equal("hello", Assert.Single(probe.Values));

        var action = Assert.Single(host.Services.GetRequiredService<IInMemoryMule>().Actions);
        Assert.Equal(DurableActionStatus.Completed, action.Status);

        var diagnostics = await host.Services.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync();
        Assert.True(diagnostics.ThroughputPerMinute >= 1);
        Assert.True(diagnostics.RuntimeCompleted >= 1);
        Assert.True(diagnostics.CompletedPerMinuteByLane[MuleSettings.DefaultLane] >= 1);
        Assert.True(diagnostics.CompletedPerMinuteByActionKey[Key] >= 1);
    }

    [Fact]
    public async Task HostedService_Should_Mark_Failed_After_Max_Attempts()
    {
        using var host = CreateHost(fail: true);
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
        await client.EnqueueAsync(Key, new TestPayload("boom"));

        await Task.Delay(500);
        await host.StopAsync();

        var action = Assert.Single(host.Services.GetRequiredService<IInMemoryMule>().Actions);
        Assert.Equal(DurableActionStatus.Failed, action.Status);
        Assert.Equal(1, action.Attempts);
        Assert.Contains("planned failure", action.LastError);
    }

    [Fact]
    public async Task ScheduledRecovery_Should_Retry_Failed_Action_Without_Polling()
    {
        using var host = CreateHost(failOnce: true, configureSettings: settings =>
        {
            settings.RecoveryMode = MuleRecoveryMode.Scheduled;
            settings.DispatchInterval = TimeSpan.FromHours(1);
            settings.RetryDelay = TimeSpan.FromMilliseconds(50);
            settings.MaxAttempts = 2;
        });
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
        await client.EnqueueAsync(Key, new TestPayload("retry"));

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitAsync();
        await host.StopAsync();

        Assert.Equal("retry", Assert.Single(probe.Values));

        var action = Assert.Single(host.Services.GetRequiredService<IInMemoryMule>().Actions);
        Assert.Equal(DurableActionStatus.Completed, action.Status);
        Assert.Equal(1, action.Attempts);
    }

    [Fact]
    public async Task ScheduledRecovery_Should_Process_Enqueued_Action_When_ImmediateDispatch_Is_Disabled()
    {
        using var host = CreateHost(configureSettings: settings =>
        {
            settings.ImmediateDispatch = false;
            settings.RecoveryMode = MuleRecoveryMode.Scheduled;
            settings.DispatchInterval = TimeSpan.FromHours(1);
        });
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
        await client.EnqueueAsync(Key, new TestPayload("scheduled"));

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitAsync();
        await host.StopAsync();

        Assert.Equal("scheduled", Assert.Single(probe.Values));
    }

    [Fact]
    public async Task DisabledCleanup_Should_Keep_Completed_Actions()
    {
        using var host = CreateHost(configureSettings: settings =>
        {
            settings.CleanupMode = MuleCleanupMode.Disabled;
            settings.CompletedRetention = TimeSpan.FromMilliseconds(1);
            settings.CleanupInterval = TimeSpan.FromMilliseconds(10);
        });
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
        await client.EnqueueAsync(Key, new TestPayload("audit"));

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitAsync();
        await Task.Delay(100);
        await host.StopAsync();

        var action = Assert.Single(host.Services.GetRequiredService<IInMemoryMule>().Actions);
        Assert.Equal(DurableActionStatus.Completed, action.Status);
    }

    [Fact]
    public async Task ScheduledCleanup_Should_Remove_Completed_Actions_When_Retention_Expires()
    {
        using var host = CreateHost(configureSettings: settings =>
        {
            settings.CleanupMode = MuleCleanupMode.Scheduled;
            settings.CompletedRetention = TimeSpan.FromMilliseconds(50);
            settings.CleanupInterval = TimeSpan.FromHours(1);
        });
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
        await client.EnqueueAsync(Key, new TestPayload("cleanup"));

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitAsync();
        await WaitForNoActionsAsync(host);
        await host.StopAsync();

        Assert.Empty(host.Services.GetRequiredService<IInMemoryMule>().Actions);
    }

    [Fact]
    public async Task EnqueueAsync_Should_Ignore_Concurrent_Duplicates()
    {
        using var host = CreateHost();
        var scopeFactory = host.Services.GetRequiredService<IServiceScopeFactory>();

        var ids = await Task.WhenAll(Enumerable.Range(0, 25).Select(async _ =>
        {
            using var scope = scopeFactory.CreateScope();
            var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
            return await client.EnqueueAsync(
                    Key,
                    new TestPayload("duplicate"),
                    options => options.DeduplicationKey = "order-1001")
                .AsTask();
        }));

        var actions = host.Services.GetRequiredService<IInMemoryMule>().Actions;
        var diagnostics = await host.Services.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync();

        Assert.Single(actions);
        Assert.Single(ids.Distinct());
        Assert.Equal(ids[0], Assert.Single(actions).Id);
        Assert.Equal(24, diagnostics.DuplicatesIgnored);
    }

    [Fact]
    public async Task HostedService_Should_Execute_Actions_In_Parallel()
    {
        using var host = CreateHost(configureSettings: settings =>
        {
            settings.WorkerCount = 4;
            settings.MaxDegreeOfParallelism = 4;
        });
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
        var startedOnUtc = DateTimeOffset.UtcNow;

        await Task.WhenAll(Enumerable.Range(0, 4).Select(index =>
            client.EnqueueAsync(Key, new TestPayload($"parallel-{index}", 250)).AsTask()));

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitForCountAsync(4);
        var elapsed = DateTimeOffset.UtcNow - startedOnUtc;
        await host.StopAsync();

        Assert.True(elapsed < TimeSpan.FromMilliseconds(900), $"Expected parallel execution, but elapsed time was {elapsed}.");
        Assert.Equal(4, probe.Values.Count);
    }

    [Fact]
    public async Task QueueWorkers_Should_Not_Wait_For_Slow_Action_Execution()
    {
        using var host = CreateHost(configureSettings: settings =>
        {
            settings.WorkerCount = 1;
            settings.MaxDegreeOfParallelism = 10;
        });
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
        var startedOnUtc = DateTimeOffset.UtcNow;

        await Task.WhenAll(Enumerable.Range(0, 10).Select(index =>
            client.EnqueueAsync(Key, new TestPayload($"admit-{index}", 500)).AsTask()));

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitForStartedCountAsync(10, TimeSpan.FromSeconds(2));
        var admissionElapsed = DateTimeOffset.UtcNow - startedOnUtc;
        await probe.WaitForCountAsync(10, TimeSpan.FromSeconds(3));
        await host.StopAsync();

        Assert.True(admissionElapsed < TimeSpan.FromMilliseconds(450), $"Reader waited on execution for {admissionElapsed}.");
    }

    [Fact]
    public async Task MaxDegreeOfParallelism_Should_Limit_Real_Execution()
    {
        using var host = CreateHost(configureSettings: settings =>
        {
            settings.WorkerCount = 4;
            settings.MaxDegreeOfParallelism = 2;
        });
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();

        await Task.WhenAll(Enumerable.Range(0, 10).Select(index =>
            client.EnqueueAsync(Key, new TestPayload($"limited-{index}", 100)).AsTask()));

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitForCountAsync(10, TimeSpan.FromSeconds(5));
        await host.StopAsync();

        Assert.True(probe.MaxExecuting <= 2, $"Expected at most 2 executing actions, but saw {probe.MaxExecuting}.");
    }

    [Fact]
    public async Task ExecutionQueueCapacity_Should_Apply_Bounded_Backpressure()
    {
        using var host = CreateHost(configureSettings: settings =>
        {
            settings.WorkerCount = 4;
            settings.MaxDegreeOfParallelism = 1;
            settings.ExecutionQueueCapacity = 5;
        });
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();

        await Task.WhenAll(Enumerable.Range(0, 25).Select(index =>
            client.EnqueueAsync(Key, new TestPayload($"backpressure-{index}", 50)).AsTask()));

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitForCountAsync(25, TimeSpan.FromSeconds(5));
        await host.StopAsync();

        var diagnostics = await host.Services.GetRequiredService<IMuleDiagnostics>().GetSnapshotAsync();
        Assert.True(diagnostics.ExecutorSaturationByLane[MuleSettings.DefaultLane] > 0);
        Assert.Equal(25, probe.Values.Count);
    }

    [Fact]
    public async Task Lanes_Should_Allow_Fast_Lane_To_Progress_When_Slow_Lane_Is_Busy()
    {
        using var host = CreateHost(configureSettings: settings =>
        {
            settings.ImmediateDispatch = false;
            settings.RecoveryMode = MuleRecoveryMode.Polling;
            settings.DispatchInterval = TimeSpan.FromMilliseconds(25);
            settings.Lanes["slow"] = new MuleLaneSettings
            {
                WorkerCount = 1,
                MaxDegreeOfParallelism = 1,
                DispatchBatchSize = 1
            };
            settings.Lanes["fast"] = new MuleLaneSettings
            {
                WorkerCount = 1,
                MaxDegreeOfParallelism = 1,
                DispatchBatchSize = 1,
                Priority = 10
            };
        });
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
        var startedOnUtc = DateTimeOffset.UtcNow;

        await client.EnqueueAsync(
            Key,
            new TestPayload("slow", 500),
            options => options.Lane = "slow");
        await client.EnqueueAsync(
            Key,
            new TestPayload("fast", 0),
            options => options.Lane = "fast");

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitForValueAsync("fast");
        var elapsed = DateTimeOffset.UtcNow - startedOnUtc;
        await host.StopAsync();

        Assert.True(elapsed < TimeSpan.FromMilliseconds(450), $"Fast lane was blocked by slow lane for {elapsed}.");
    }

    [Fact]
    public async Task ImmediateDispatch_Should_Run_Configured_Lane_Workers_Independently()
    {
        using var host = CreateHost(configureSettings: settings =>
        {
            settings.Lanes["slow"] = new MuleLaneSettings
            {
                WorkerCount = 1,
                MaxDegreeOfParallelism = 1
            };
            settings.Lanes["fast"] = new MuleLaneSettings
            {
                WorkerCount = 1,
                MaxDegreeOfParallelism = 1,
                Priority = 10,
                Weight = 5
            };
        });
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
        var startedOnUtc = DateTimeOffset.UtcNow;

        await client.EnqueueAsync(
            Key,
            new TestPayload("slow-immediate", 500),
            options => options.Lane = "slow");
        await client.EnqueueAsync(
            Key,
            new TestPayload("fast-immediate", 0),
            options => options.Lane = "fast");

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitForValueAsync("fast-immediate");
        var elapsed = DateTimeOffset.UtcNow - startedOnUtc;
        await host.StopAsync();

        Assert.True(elapsed < TimeSpan.FromMilliseconds(450), $"Fast lane immediate worker was blocked for {elapsed}.");
    }

    [Fact]
    public async Task ScheduledRecovery_Should_Drain_Until_Empty_When_Configured()
    {
        using var host = CreateHost(configureSettings: settings =>
        {
            settings.ImmediateDispatch = false;
            settings.RecoveryMode = MuleRecoveryMode.Scheduled;
            settings.DispatchInterval = TimeSpan.FromHours(1);
            settings.DispatchBatchSize = 1;
            settings.DrainUntilEmpty = true;
            settings.MaxDrainActionsPerCycle = 10;
        });

        using (var scope = host.Services.CreateScope())
        {
            var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
            for (var index = 0; index < 5; index++)
                await client.EnqueueAsync(Key, new TestPayload($"drain-{index}"));
        }

        await host.StartAsync();

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitForCountAsync(5);
        await host.StopAsync();

        Assert.Equal(5, probe.Values.Count);
    }

    [Fact]
    public async Task ScheduledRecovery_Should_Use_Parallel_Executor()
    {
        using var host = CreateHost(configureSettings: settings =>
        {
            settings.ImmediateDispatch = false;
            settings.RecoveryMode = MuleRecoveryMode.Scheduled;
            settings.DispatchInterval = TimeSpan.FromHours(1);
            settings.WorkerCount = 1;
            settings.MaxDegreeOfParallelism = 10;
            settings.DispatchBatchSize = 100;
        });

        using (var scope = host.Services.CreateScope())
        {
            var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
            for (var index = 0; index < 100; index++)
                await client.EnqueueAsync(Key, new TestPayload($"recovery-{index}", 100));
        }

        await host.StartAsync();

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitForStartedCountAsync(10, TimeSpan.FromSeconds(2));
        await probe.WaitForCountAsync(100, TimeSpan.FromSeconds(5));
        await host.StopAsync();

        Assert.True(probe.MaxExecuting > 1);
        Assert.True(probe.MaxExecuting <= 10);
    }


    [Fact]
    public async Task HostedService_Should_Process_Thousands_Of_Actions()
    {
        using var host = CreateHost(configureSettings: settings =>
        {
            settings.WorkerCount = 8;
            settings.MaxDegreeOfParallelism = 16;
            settings.DispatchBatchSize = 100;
        });
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();

        await Task.WhenAll(Enumerable.Range(0, 2_000).Select(index =>
            client.EnqueueAsync(Key, new TestPayload($"stress-{index}")).AsTask()));

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitForCountAsync(2_000, TimeSpan.FromSeconds(10));
        await host.StopAsync();

        Assert.Equal(2_000, probe.Values.Count);
    }

    private static IHost CreateHost(
        bool fail = false,
        bool failOnce = false,
        Action<MuleSettings> configureSettings = null)
        => Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.Configure<MuleSettings>(settings =>
                {
                    settings.DispatchInterval = TimeSpan.FromMilliseconds(50);
                    settings.RetryDelay = TimeSpan.FromMilliseconds(50);
                    settings.MaxAttempts = 1;
                    configureSettings?.Invoke(settings);
                });

                services.AddSingleton(new TestProbe(fail, failOnce));
                services.AddMule(mule => mule
                    .UseInMemory()
                    .AddActionsFromAssemblyContaining<CaptureTestPayloadAction>());
            })
            .Build();

    private static async Task WaitForNoActionsAsync(IHost host)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        while (!timeout.IsCancellationRequested)
        {
            if (host.Services.GetRequiredService<IInMemoryMule>().Actions.Count == 0)
                return;

            await Task.Delay(25, timeout.Token);
        }
    }

    private sealed record TestPayload(string Value, int DelayMilliseconds = 0);

    [MuleAction("tests.capture.v1")]
    private sealed class CaptureTestPayloadAction : IMuleAction<TestPayload>
    {
        private readonly TestProbe _probe;

        public CaptureTestPayloadAction(TestProbe probe)
        {
            _probe = probe;
        }

        public async ValueTask ExecuteAsync(MuleActionContext<TestPayload> context, CancellationToken cancellationToken)
        {
            _probe.Started();
            try
            {
                if (_probe.ShouldFail())
                    throw new InvalidOperationException("planned failure");

                if (context.Payload.DelayMilliseconds > 0)
                    await Task.Delay(context.Payload.DelayMilliseconds, cancellationToken);

                _probe.Values.Add(context.Payload.Value);
                _probe.Signal(context.Payload.Value);
            }
            finally
            {
                _probe.Finished();
            }
        }
    }

    private sealed class TestProbe
    {
        private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ConcurrentDictionary<string, TaskCompletionSource> _valueCompletions = new(StringComparer.Ordinal);
        private int _remainingFailures;
        private int _executing;
        private int _maxExecuting;
        private int _started;

        public TestProbe(bool fail, bool failOnce)
        {
            Fail = fail;
            _remainingFailures = failOnce ? 1 : 0;
        }

        public bool Fail { get; }

        public ConcurrentBag<string> Values { get; } = new();

        public int MaxExecuting => Volatile.Read(ref _maxExecuting);

        public bool ShouldFail()
        {
            if (Fail)
                return true;

            return _remainingFailures > 0 && Interlocked.Decrement(ref _remainingFailures) >= 0;
        }

        public void Started()
        {
            var executing = Interlocked.Increment(ref _executing);
            Interlocked.Increment(ref _started);

            while (true)
            {
                var current = Volatile.Read(ref _maxExecuting);
                if (executing <= current ||
                    Interlocked.CompareExchange(ref _maxExecuting, executing, current) == current)
                    return;
            }
        }

        public void Finished()
            => Interlocked.Decrement(ref _executing);

        public void Signal(string value)
        {
            _completion.TrySetResult();
            _valueCompletions
                .GetOrAdd(value, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))
                .TrySetResult();
        }

        public async Task WaitForCountAsync(int count)
            => await WaitForCountAsync(count, TimeSpan.FromSeconds(3));

        public async Task WaitForCountAsync(int count, TimeSpan timeout)
        {
            using var timeoutSource = new CancellationTokenSource(timeout);

            while (!timeoutSource.IsCancellationRequested)
            {
                if (Values.Count >= count)
                    return;

                await Task.Delay(25, timeoutSource.Token);
            }
        }

        public async Task WaitForStartedCountAsync(int count, TimeSpan timeout)
        {
            using var timeoutSource = new CancellationTokenSource(timeout);

            while (!timeoutSource.IsCancellationRequested)
            {
                if (Volatile.Read(ref _started) >= count)
                    return;

                await Task.Delay(10, timeoutSource.Token);
            }
        }

        public async Task WaitForValueAsync(string value)
        {
            var completion = _valueCompletions.GetOrAdd(
                value,
                _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

            await completion.Task.WaitAsync(TimeSpan.FromSeconds(3));
        }

        private void Signal()
            => _completion.TrySetResult();

        public async Task WaitAsync()
            => await _completion.Task.WaitAsync(TimeSpan.FromSeconds(3));
    }
}
