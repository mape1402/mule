namespace Mule.Testing.Tests;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule.InMemory;

public sealed class MuleTestingTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.testing.capture.v1");

    [Fact]
    public async Task UseTesting_Should_Register_Test_Harness_And_Run_Actions()
    {
        using var host = CreateHost();
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
        var actionId = await client.EnqueueAsync(Key, new TestPayload("hello"));

        var harness = host.Services.GetRequiredService<IMuleTestHarness>();
        var action = await harness.WaitForActionAsync(actionId, DurableActionStatus.Completed);

        await host.StopAsync();

        Assert.Equal(Key, action.Key);
        Assert.Equal(DurableActionStatus.Completed, action.Status);
        Assert.Equal("hello", Assert.Single(host.Services.GetRequiredService<TestProbe>().Values));
    }

    [Fact]
    public async Task WaitForActionAsync_Should_Timeout_When_Action_Is_Not_Observed()
    {
        using var host = CreateHost();

        var harness = host.Services.GetRequiredService<IMuleTestHarness>();

        await Assert.ThrowsAsync<TimeoutException>(() =>
            harness.WaitForActionAsync(
                ActionKey.From("missing.action"),
                DurableActionStatus.Completed,
                TimeSpan.FromMilliseconds(50)));
    }

    [Fact]
    public async Task Harness_Should_Wait_By_Key_And_Respect_Cancellation()
    {
        var action = new DurableAction
        {
            Key = Key,
            Status = DurableActionStatus.Pending,
            CreatedOnUtc = DateTimeOffset.UtcNow
        };
        var harness = new MuleTestHarness(new FixedInMemoryMule([action]));

        Assert.Throws<ArgumentNullException>(() => MuleTestingRegistrationBuilderExtensions.UseTesting(null));
        Assert.Throws<ArgumentNullException>(() => new MuleTestHarness(null));
        Assert.Same(action, await harness.WaitForActionAsync(Key));

        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        await Assert.ThrowsAsync<TimeoutException>(() =>
            harness.WaitForActionAsync(
                Ulid.NewUlid(),
                null,
                TimeSpan.FromSeconds(1),
                cancellation.Token));
    }

    private static IHost CreateHost()
        => Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton<TestProbe>();
                services.AddMule(mule => mule
                    .UseTesting()
                    .AddActionsFromAssemblyContaining<CaptureTestPayloadAction>());
            })
            .Build();

    private sealed record TestPayload(string Value);

    [MuleAction("tests.testing.capture.v1")]
    private sealed class CaptureTestPayloadAction : IMuleAction<TestPayload>
    {
        private readonly TestProbe _probe;

        public CaptureTestPayloadAction(TestProbe probe)
        {
            _probe = probe;
        }

        public ValueTask ExecuteAsync(MuleActionContext<TestPayload> context, CancellationToken cancellationToken)
        {
            _probe.Values.Add(context.Payload.Value);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestProbe
    {
        public List<string> Values { get; } = new();
    }

    private sealed class FixedInMemoryMule : IInMemoryMule
    {
        public FixedInMemoryMule(IReadOnlyCollection<DurableAction> actions)
        {
            Actions = actions;
        }

        public IReadOnlyCollection<DurableAction> Actions { get; }
    }
}
