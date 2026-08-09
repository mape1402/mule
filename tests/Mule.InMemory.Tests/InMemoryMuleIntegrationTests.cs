namespace Mule.InMemory.Tests;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

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

    private sealed record TestPayload(string Value);

    [MuleAction("tests.capture.v1")]
    private sealed class CaptureTestPayloadAction : IMuleAction<TestPayload>
    {
        private readonly TestProbe _probe;

        public CaptureTestPayloadAction(TestProbe probe)
        {
            _probe = probe;
        }

        public ValueTask ExecuteAsync(MuleActionContext<TestPayload> context, CancellationToken cancellationToken)
        {
            if (_probe.ShouldFail())
                throw new InvalidOperationException("planned failure");

            _probe.Values.Add(context.Payload.Value);
            _probe.Signal();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestProbe
    {
        private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _remainingFailures;

        public TestProbe(bool fail, bool failOnce)
        {
            Fail = fail;
            _remainingFailures = failOnce ? 1 : 0;
        }

        public bool Fail { get; }

        public List<string> Values { get; } = new();

        public bool ShouldFail()
        {
            if (Fail)
                return true;

            return _remainingFailures > 0 && Interlocked.Decrement(ref _remainingFailures) >= 0;
        }

        public void Signal()
            => _completion.TrySetResult();

        public async Task WaitAsync()
            => await _completion.Task.WaitAsync(TimeSpan.FromSeconds(3));
    }
}
