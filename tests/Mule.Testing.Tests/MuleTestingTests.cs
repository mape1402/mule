namespace Mule.Testing.Tests;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule.InMemory;
using NSubstitute;

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
        var replayService = Substitute.For<IMuleReplayService>();
        var harness = new MuleTestHarness(new FixedInMemoryMule([action]), replayService);

        Assert.Throws<ArgumentNullException>(() => MuleTestingRegistrationBuilderExtensions.UseTesting(null));
        Assert.Throws<ArgumentNullException>(() => new MuleTestHarness(null, replayService));
        Assert.Throws<ArgumentNullException>(() => new MuleTestHarness(new FixedInMemoryMule([]), null));
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

    [Fact]
    public async Task Harness_Should_Schedule_And_Wait_For_Replay()
    {
        var sourceId = Ulid.NewUlid();
        var requestId = Ulid.NewUlid();
        var actionId = Ulid.NewUlid();
        var info = new MuleReplayInfo
        {
            RequestId = requestId,
            ReplayOfActionId = sourceId,
            ReplayRootActionId = sourceId,
            ReplayActionId = actionId,
            ReplaySequence = 1,
            Status = MuleReplayStatus.Materialized,
            ActionKey = Key,
            RequestedOnUtc = DateTimeOffset.UtcNow,
            ScheduleOnUtc = DateTimeOffset.UtcNow
        };
        var replayService = Substitute.For<IMuleReplayService>();
        replayService.ReplayAsync(sourceId, Arg.Any<MuleReplayRequest>(), Arg.Any<CancellationToken>())
            .Returns(new MuleReplayResult { Outcome = MuleReplayOutcome.Scheduled, Replay = info });
        replayService.GetReplayAsync(requestId, Arg.Any<CancellationToken>()).Returns(info);
        var harness = new MuleTestHarness(new FixedInMemoryMule([]), replayService);
        var request = new MuleReplayRequest { RequestId = requestId };

        var result = await harness.ReplayAsync(sourceId, request);
        var observed = await harness.WaitForReplayAsync(requestId, MuleReplayStatus.Materialized);

        Assert.Equal(actionId, result.ReplayActionId);
        Assert.Same(info, observed);
    }

    [Fact]
    public async Task WaitForReplayAsync_Should_Timeout_When_Replay_Is_Not_Observed()
    {
        var replayService = Substitute.For<IMuleReplayService>();
        replayService.GetReplayAsync(Arg.Any<Ulid>(), Arg.Any<CancellationToken>())
            .Returns((MuleReplayInfo)null);
        var harness = new MuleTestHarness(new FixedInMemoryMule([]), replayService);

        await Assert.ThrowsAsync<TimeoutException>(() => harness.WaitForReplayAsync(
            Ulid.NewUlid(),
            MuleReplayStatus.Materialized,
            TimeSpan.FromMilliseconds(50)));
    }

    [Fact]
    public async Task WaitForReplayAsync_Should_Describe_Timeout_Without_Required_Status()
    {
        var replayService = Substitute.For<IMuleReplayService>();
        replayService.GetReplayAsync(Arg.Any<Ulid>(), Arg.Any<CancellationToken>())
            .Returns((MuleReplayInfo)null);
        var harness = new MuleTestHarness(new FixedInMemoryMule([]), replayService);

        var error = await Assert.ThrowsAsync<TimeoutException>(() => harness.WaitForReplayAsync(
            Ulid.NewUlid(),
            timeout: TimeSpan.FromMilliseconds(50)));

        Assert.DoesNotContain("with status", error.Message);
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
