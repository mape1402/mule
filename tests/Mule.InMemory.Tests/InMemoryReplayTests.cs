namespace Mule.InMemory.Tests;

using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule.Diagnostics;
using Mule.InMemory;

public sealed class InMemoryReplayTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.replay.in-memory.v1");

    [Fact]
    public async Task ReplayAsync_Should_Materialize_And_Execute_A_Failed_Action()
    {
        using var host = CreateHost();
        await host.StartAsync();

        Ulid sourceId;
        using (var scope = host.Services.CreateScope())
        {
            var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
            sourceId = await client.EnqueueAsync(
                Key,
                new ReplayPayload("payload"),
                options =>
                {
                    options.CorrelationId = "correlation";
                    options.DeduplicationKey = "original-deduplication";
                    options.Metadata["keep"] = "original";
                    options.Metadata["replace"] = "original";
                });
        }

        await WaitForActionAsync(host, sourceId, DurableActionStatus.Failed);
        host.Services.GetRequiredService<ReplayProbe>().ShouldFail = false;

        var requestId = Ulid.NewUlid();
        MuleReplayResult result;
        using (var scope = host.Services.CreateScope())
        {
            var replayService = scope.ServiceProvider.GetRequiredService<IMuleReplayService>();
            result = await replayService.ReplayAsync(sourceId, new MuleReplayRequest
            {
                RequestId = requestId,
                RequestedBy = "support@example.test",
                Reason = "dependency recovered",
                Metadata = new Dictionary<string, string>
                {
                    ["replace"] = "replay",
                    ["added"] = "value"
                }
            });
            var duplicate = await replayService.ReplayAsync(sourceId, new MuleReplayRequest
            {
                RequestId = requestId,
                RequestedBy = "support@example.test",
                Reason = "dependency recovered",
                Metadata = new Dictionary<string, string>
                {
                    ["added"] = "value",
                    ["replace"] = "replay"
                }
            });

            Assert.Equal(MuleReplayOutcome.AlreadyScheduled, duplicate.Outcome);
            Assert.Equal(result.ReplayActionId, duplicate.ReplayActionId);
        }

        var replayed = await WaitForActionAsync(host, result.ReplayActionId, DurableActionStatus.Completed);
        var original = host.Services.GetRequiredService<IInMemoryMule>().Actions.Single(x => x.Id == sourceId);
        using var metadata = JsonDocument.Parse(replayed.Metadata);

        await host.StopAsync();

        Assert.Equal(MuleReplayOutcome.Scheduled, result.Outcome);
        Assert.Equal(DurableActionStatus.Failed, original.Status);
        Assert.Equal(0, replayed.Attempts);
        Assert.Equal("correlation", replayed.CorrelationId);
        Assert.NotEqual("original-deduplication", replayed.DeduplicationKey);
        Assert.Equal("original", metadata.RootElement.GetProperty("keep").GetString());
        Assert.Equal("replay", metadata.RootElement.GetProperty("replace").GetString());
        Assert.Equal("value", metadata.RootElement.GetProperty("added").GetString());
        Assert.Equal(["payload", "payload"], host.Services.GetRequiredService<ReplayProbe>().Values);
    }

    [Fact]
    public async Task ScheduleAsync_Should_Enforce_Eligibility_Idempotency_And_Linear_Replays()
    {
        var store = new InMemoryMuleStore();
        var storage = new InMemoryMuleStorage(store, new MuleRuntimeMetrics());
        var failed = CreateAction(DurableActionStatus.Failed);
        var pending = CreateAction(DurableActionStatus.Pending);
        store.Add(failed);
        store.Add(pending);
        var now = DateTimeOffset.UtcNow;
        var request = Request(now);

        var created = await storage.ScheduleAsync(failed.Id, request, now);
        var same = await storage.ScheduleAsync(failed.Id, request, now);
        var conflict = await storage.ScheduleAsync(failed.Id, Request(now, request.RequestId, "different"), now);
        var secondRequest = await storage.ScheduleAsync(failed.Id, Request(now), now);
        var notFound = await storage.ScheduleAsync(Ulid.NewUlid(), Request(now), now);
        var notEligible = await storage.ScheduleAsync(pending.Id, Request(now), now);

        Assert.Equal(MuleReplayOutcome.Scheduled, created.Outcome);
        Assert.Equal(MuleReplayOutcome.AlreadyScheduled, same.Outcome);
        Assert.Equal(MuleReplayOutcome.RequestConflict, conflict.Outcome);
        Assert.Equal(MuleReplayOutcome.ReplayAlreadyExists, secondRequest.Outcome);
        Assert.Equal(MuleReplayOutcome.NotFound, notFound.Outcome);
        Assert.Equal(MuleReplayOutcome.NotEligible, notEligible.Outcome);
        Assert.Null(await storage.FindAsync(Ulid.NewUlid()));
    }

    [Fact]
    public async Task Parallel_Replays_Should_Create_Explicit_Branches()
    {
        var store = new InMemoryMuleStore();
        var storage = new InMemoryMuleStorage(store, new MuleRuntimeMetrics());
        var source = CreateAction(DurableActionStatus.Failed);
        store.Add(source);
        var now = DateTimeOffset.UtcNow;
        var firstRequest = Request(now, allowParallel: true);
        var secondRequest = Request(now, allowParallel: true);

        var first = await storage.ScheduleAsync(source.Id, firstRequest, now);
        var second = await storage.ScheduleAsync(source.Id, secondRequest, now);

        Assert.Equal(MuleReplayOutcome.Scheduled, first.Outcome);
        Assert.Equal(MuleReplayOutcome.Scheduled, second.Outcome);
        Assert.NotEqual(first.ReplayActionId, second.ReplayActionId);
    }

    [Fact]
    public async Task Claim_Release_And_Materialize_Should_Be_Lease_Safe_And_Recoverable()
    {
        var store = new InMemoryMuleStore();
        var storage = new InMemoryMuleStorage(store, new MuleRuntimeMetrics());
        var source = CreateAction(DurableActionStatus.Failed);
        store.Add(source);
        var now = DateTimeOffset.UtcNow;
        var scheduled = await storage.ScheduleAsync(source.Id, Request(now), now);

        var claim = Assert.Single(await storage.ClaimScheduledAsync(10, TimeSpan.FromMinutes(1), now));
        Assert.Empty(await storage.ClaimScheduledAsync(10, TimeSpan.FromMinutes(1), now));
        Assert.Equal(now.AddMinutes(1), await storage.GetNextScheduledOnUtcAsync(TimeSpan.FromMinutes(1), now));

        var staleClaim = new MuleReplayClaim
        {
            Replay = claim.Replay,
            SourceAction = claim.SourceAction,
            LockToken = Ulid.NewUlid()
        };
        var action = Mule.Replay.MuleReplayModel.CreateAction(claim, now);
        Assert.Equal(
            MuleReplayMaterializationOutcome.LeaseLost,
            await storage.MaterializeAsync(staleClaim, action, now));

        await storage.ReleaseAsync(staleClaim, "ignored", now);
        await storage.ReleaseAsync(claim, "transient", now.AddSeconds(5));
        Assert.Empty(await storage.ClaimScheduledAsync(10, TimeSpan.FromMinutes(1), now));
        Assert.Equal(now.AddSeconds(5), await storage.GetNextScheduledOnUtcAsync(TimeSpan.FromMinutes(1), now));

        var retryClaim = Assert.Single(await storage.ClaimScheduledAsync(10, TimeSpan.FromMinutes(1), now.AddSeconds(5)));
        action = Mule.Replay.MuleReplayModel.CreateAction(retryClaim, now.AddSeconds(5));
        Assert.Equal(
            MuleReplayMaterializationOutcome.Created,
            await storage.MaterializeAsync(retryClaim, action, now.AddSeconds(5)));
        Assert.Equal(
            MuleReplayMaterializationOutcome.Reconciled,
            await storage.MaterializeAsync(retryClaim, action, now.AddSeconds(5)));
        Assert.Null(await storage.GetNextScheduledOnUtcAsync(TimeSpan.FromMinutes(1), now.AddSeconds(5)));

        var info = await storage.FindAsync(scheduled.Replay.RequestId);
        Assert.Equal(MuleReplayStatus.Materialized, info.Status);
        Assert.Equal(2, info.MaterializationAttempts);
        Assert.Null(info.LastError);
    }

    [Fact]
    public async Task Materialization_Should_Reject_Missing_Replay_And_Action_Collisions()
    {
        var store = new InMemoryMuleStore();
        var storage = new InMemoryMuleStorage(store, new MuleRuntimeMetrics());
        var source = CreateAction(DurableActionStatus.Failed);
        store.Add(source);
        var collision = CreateAction(DurableActionStatus.Pending);
        collision.DeduplicationKey = "collision";
        store.Add(collision);
        var now = DateTimeOffset.UtcNow;
        await storage.ScheduleAsync(source.Id, Request(now), now);
        var claim = Assert.Single(await storage.ClaimScheduledAsync(1, TimeSpan.FromMinutes(1), now));
        var missing = new MuleReplayClaim
        {
            Replay = new MuleReplay { RequestId = Ulid.NewUlid() },
            SourceAction = source,
            LockToken = Ulid.NewUlid()
        };

        Assert.Equal(
            MuleReplayMaterializationOutcome.LeaseLost,
            await storage.MaterializeAsync(missing, CreateAction(DurableActionStatus.Pending), now));

        var duplicateId = Mule.Replay.MuleReplayModel.CreateAction(claim, now);
        duplicateId.Id = source.Id;
        await Assert.ThrowsAsync<InvalidOperationException>(() => storage.MaterializeAsync(claim, duplicateId, now));

        var duplicateKey = Mule.Replay.MuleReplayModel.CreateAction(claim, now);
        duplicateKey.DeduplicationKey = collision.DeduplicationKey;
        await Assert.ThrowsAsync<InvalidOperationException>(() => storage.MaterializeAsync(claim, duplicateKey, now));
    }

    [Fact]
    public async Task Claim_Should_Skip_Replay_Whose_Source_Was_Removed()
    {
        var store = new InMemoryMuleStore();
        var storage = new InMemoryMuleStorage(store, new MuleRuntimeMetrics());
        var source = CreateAction(DurableActionStatus.Failed);
        store.Add(source);
        var now = DateTimeOffset.UtcNow;
        await storage.ScheduleAsync(source.Id, Request(now), now);
        store.MarkCompleted(source.Id, now.AddMinutes(-1));
        store.CleanCompleted(now, 1);

        Assert.Empty(await storage.ClaimScheduledAsync(1, TimeSpan.FromMinutes(1), now));
    }

    [Fact]
    public async Task Replaying_A_Replayed_Failure_Should_Advance_The_Trace()
    {
        var store = new InMemoryMuleStore();
        var storage = new InMemoryMuleStorage(store, new MuleRuntimeMetrics());
        var root = CreateAction(DurableActionStatus.Failed);
        store.Add(root);
        var now = DateTimeOffset.UtcNow;
        var first = await storage.ScheduleAsync(root.Id, Request(now), now);
        var firstClaim = Assert.Single(await storage.ClaimScheduledAsync(1, TimeSpan.FromMinutes(1), now));
        var child = Mule.Replay.MuleReplayModel.CreateAction(firstClaim, now);
        await storage.MaterializeAsync(firstClaim, child, now);
        store.MarkFailed(child.Id, "failed again", null);

        var second = await storage.ScheduleAsync(child.Id, Request(now.AddSeconds(1)), now.AddSeconds(1));

        Assert.Equal(root.Id, second.Replay.ReplayRootActionId);
        Assert.Equal(first.ReplayActionId, second.OriginalActionId);
        Assert.Equal(2, second.Replay.ReplaySequence);
    }

    private static IHost CreateHost()
        => Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton<ReplayProbe>();
                services.AddMule(mule => mule
                    .Configure(settings =>
                    {
                        settings.MaxAttempts = 1;
                        settings.DispatchInterval = TimeSpan.FromMilliseconds(10);
                        settings.Replay.RetryDelay = TimeSpan.FromMilliseconds(10);
                    })
                    .UseInMemory()
                    .AddActionsFromAssemblyContaining<ReplayAction>());
            })
            .Build();

    private static async Task<DurableAction> WaitForActionAsync(
        IHost host,
        Ulid id,
        DurableActionStatus status)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!timeout.IsCancellationRequested)
        {
            var action = host.Services.GetRequiredService<IInMemoryMule>().Actions.SingleOrDefault(x => x.Id == id);
            if (action?.Status == status)
                return action;

            await Task.Delay(10, timeout.Token);
        }

        throw new TimeoutException($"Action '{id}' did not reach '{status}'.");
    }

    private static DurableAction CreateAction(DurableActionStatus status)
        => new()
        {
            Key = Key,
            Payload = "{}",
            PayloadType = typeof(ReplayPayload).AssemblyQualifiedName,
            Status = status,
            CreatedOnUtc = DateTimeOffset.UtcNow,
            TerminalOnUtc = status == DurableActionStatus.Failed ? DateTimeOffset.UtcNow : null
        };

    private static MuleReplayRequest Request(
        DateTimeOffset now,
        Ulid? requestId = null,
        string reason = "fixed",
        bool allowParallel = false)
        => new()
        {
            RequestId = requestId ?? Ulid.NewUlid(),
            RequestedBy = "support",
            Reason = reason,
            ScheduleOnUtc = now,
            Metadata = new Dictionary<string, string>(),
            AllowParallelReplay = allowParallel
        };

    private sealed record ReplayPayload(string Value);

    [MuleAction("tests.replay.in-memory.v1")]
    private sealed class ReplayAction : IMuleAction<ReplayPayload>
    {
        private readonly ReplayProbe _probe;

        public ReplayAction(ReplayProbe probe)
            => _probe = probe;

        public ValueTask ExecuteAsync(MuleActionContext<ReplayPayload> context, CancellationToken cancellationToken)
        {
            _probe.Values.Add(context.Payload.Value);
            return _probe.ShouldFail
                ? ValueTask.FromException(new InvalidOperationException("expected failure"))
                : ValueTask.CompletedTask;
        }
    }

    private sealed class ReplayProbe
    {
        public bool ShouldFail { get; set; } = true;

        public List<string> Values { get; } = new();
    }
}
