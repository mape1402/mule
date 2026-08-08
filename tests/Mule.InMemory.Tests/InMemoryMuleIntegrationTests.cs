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

    private static IHost CreateHost(bool fail = false)
        => Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.Configure<MuleSettings>(settings =>
                {
                    settings.DispatchInterval = TimeSpan.FromMilliseconds(50);
                    settings.RetryDelay = TimeSpan.FromMilliseconds(50);
                    settings.MaxAttempts = 1;
                });

                services.AddSingleton(new TestProbe(fail));
                services.AddMule(mule =>
                {
                    mule.AddActionsFromAssemblyContaining<CaptureTestPayloadAction>();
                });
                services.UseInMemoryMule();
            })
            .Build();

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
            if (_probe.Fail)
                throw new InvalidOperationException("planned failure");

            _probe.Values.Add(context.Payload.Value);
            _probe.Signal();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestProbe
    {
        private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TestProbe(bool fail)
        {
            Fail = fail;
        }

        public bool Fail { get; }

        public List<string> Values { get; } = new();

        public void Signal()
            => _completion.TrySetResult();

        public async Task WaitAsync()
            => await _completion.Task.WaitAsync(TimeSpan.FromSeconds(3));
    }
}
