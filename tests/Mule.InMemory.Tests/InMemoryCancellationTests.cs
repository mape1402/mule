namespace Mule.InMemory.Tests;

using System.Reflection;
using Mule.Diagnostics;
using Mule.InMemory;

public sealed class InMemoryCancellationTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.cancel.inmemory.v1");

    [Fact]
    public void Cancel_Should_Be_Idempotent_And_Remove_Action_From_Backlog()
    {
        var store = new InMemoryMuleStore();
        var now = DateTimeOffset.UtcNow;
        var action = CreateAction(DurableActionStatus.Pending);
        Assert.True(store.Add(action));

        var first = store.Cancel(action.Id, now);
        var second = store.Cancel(action.Id, now.AddMinutes(1));
        var snapshot = store.GetSnapshot(
            TimeSpan.FromMinutes(5),
            new MuleRuntimeMetrics().GetSnapshot(now));

        Assert.Equal(MuleCancellationOutcome.Cancelled, first.Outcome);
        Assert.Equal(now, first.CancelledOnUtc);
        Assert.Equal(MuleCancellationOutcome.AlreadyCancelled, second.Outcome);
        Assert.Equal(now, second.CancelledOnUtc);
        Assert.Null(store.Lock(action.Id, TimeSpan.FromMinutes(5), now));
        Assert.Empty(store.ClaimPending(MuleSettings.DefaultLane, 10, TimeSpan.FromMinutes(5), now));
        Assert.Equal(1, snapshot.Cancelled);
        Assert.Equal(0, snapshot.Pending);
        Assert.Empty(snapshot.BacklogByLane);
        Assert.Equal(0, store.CleanCompleted(now.AddDays(1), 100));
    }

    [Theory]
    [InlineData(DurableActionStatus.Locked)]
    [InlineData(DurableActionStatus.Completed)]
    [InlineData(DurableActionStatus.Failed)]
    public void Cancel_Should_Reject_NonPending_Action(DurableActionStatus status)
    {
        var store = new InMemoryMuleStore();
        var action = CreateAction(status);
        Assert.True(store.Add(action));

        var result = store.Cancel(action.Id, DateTimeOffset.UtcNow);

        Assert.Equal(MuleCancellationOutcome.TooLate, result.Outcome);
    }

    [Fact]
    public void Cancel_Should_Return_NotFound_For_Unknown_Action()
    {
        var result = new InMemoryMuleStore().Cancel(Ulid.NewUlid(), DateTimeOffset.UtcNow);

        Assert.Equal(MuleCancellationOutcome.NotFound, result.Outcome);
    }

    [Fact]
    public void Cancel_Should_Cancel_Scheduled_Replay_By_Reserved_Action_Id()
    {
        var store = new InMemoryMuleStore();
        var now = DateTimeOffset.UtcNow;
        var source = CreateAction(DurableActionStatus.Failed);
        Assert.True(store.Add(source));
        var scheduled = store.ScheduleReplay(source.Id, new MuleReplayRequest
        {
            RequestId = Ulid.NewUlid(),
            ScheduleOnUtc = now.AddHours(1)
        }, now);
        Assert.Equal(
            now.AddHours(1),
            store.GetNextScheduledReplayOnUtc(TimeSpan.FromMinutes(5), now));

        var result = store.Cancel(scheduled.ReplayActionId, now.AddMinutes(1));
        var replay = store.FindReplay(scheduled.Replay.RequestId);

        Assert.Equal(MuleCancellationOutcome.Cancelled, result.Outcome);
        Assert.Equal(MuleReplayStatus.Cancelled, replay.Status);
        Assert.Equal(result.CancelledOnUtc, replay.CancelledOnUtc);
        Assert.Equal(MuleCancellationOutcome.AlreadyCancelled, store.Cancel(scheduled.ReplayActionId, now.AddMinutes(2)).Outcome);
        Assert.Empty(store.ClaimScheduledReplays(10, TimeSpan.FromMinutes(5), now.AddHours(2)));
    }

    [Fact]
    public void Cancel_Should_Reject_Replay_After_It_Is_Claimed()
    {
        var store = new InMemoryMuleStore();
        var now = DateTimeOffset.UtcNow;
        var source = CreateAction(DurableActionStatus.Failed);
        Assert.True(store.Add(source));
        var scheduled = store.ScheduleReplay(source.Id, new MuleReplayRequest
        {
            RequestId = Ulid.NewUlid(),
            ScheduleOnUtc = now
        }, now);
        Assert.Single(store.ClaimScheduledReplays(1, TimeSpan.FromMinutes(5), now));

        var result = store.Cancel(scheduled.ReplayActionId, now);

        Assert.Equal(MuleCancellationOutcome.TooLate, result.Outcome);
    }

    [Fact]
    public void Cancel_Should_Reject_Replay_With_NonScheduled_Status()
    {
        var store = new InMemoryMuleStore();
        var now = DateTimeOffset.UtcNow;
        var source = CreateAction(DurableActionStatus.Failed);
        Assert.True(store.Add(source));
        var scheduled = store.ScheduleReplay(source.Id, new MuleReplayRequest
        {
            RequestId = Ulid.NewUlid(),
            ScheduleOnUtc = now.AddHours(1)
        }, now);
        var replays = (List<MuleReplay>)typeof(InMemoryMuleStore)
            .GetField("_replays", BindingFlags.Instance | BindingFlags.NonPublic)
            .GetValue(store);
        replays.Single(x => x.RequestId == scheduled.Replay.RequestId).Status = MuleReplayStatus.Materialized;

        var result = store.Cancel(scheduled.ReplayActionId, now);

        Assert.Equal(MuleCancellationOutcome.TooLate, result.Outcome);
    }

    [Fact]
    public void MarkCancelled_Should_Only_Complete_Owned_NonTerminal_Actions()
    {
        var store = new InMemoryMuleStore();
        var pending = CreateAction(DurableActionStatus.Pending);
        var locked = CreateAction(DurableActionStatus.Locked);
        var completed = CreateAction(DurableActionStatus.Completed);
        Assert.True(store.Add(pending));
        Assert.True(store.Add(locked));
        Assert.True(store.Add(completed));

        Assert.True(store.MarkCancelled(pending.Id, DateTimeOffset.UtcNow));
        Assert.True(store.MarkCancelled(locked.Id, DateTimeOffset.UtcNow));
        Assert.True(store.MarkCancelled(pending.Id, DateTimeOffset.UtcNow));
        Assert.False(store.MarkCancelled(completed.Id, DateTimeOffset.UtcNow));
        Assert.False(store.MarkCancelled(Ulid.NewUlid(), DateTimeOffset.UtcNow));
    }

    [Fact]
    public async Task Storage_Should_Expose_Public_And_Terminal_Cancellation_Capabilities()
    {
        var store = new InMemoryMuleStore();
        var storage = new InMemoryMuleStorage(store, new MuleRuntimeMetrics());
        var pending = CreateAction(DurableActionStatus.Pending);
        var locked = CreateAction(DurableActionStatus.Locked);
        await storage.AddRangeAsync([pending, locked]);
        await storage.SaveChangesAsync();

        var cancelled = await storage.CancelAsync(pending.Id, DateTimeOffset.UtcNow);
        var terminal = await storage.MarkCancelledAsync(locked.Id, DateTimeOffset.UtcNow);

        Assert.Equal(MuleCancellationOutcome.Cancelled, cancelled.Outcome);
        Assert.True(terminal);
    }

    private static DurableAction CreateAction(DurableActionStatus status)
        => new()
        {
            Id = Ulid.NewUlid(),
            ConcurrencyToken = Ulid.NewUlid(),
            Key = Key,
            Lane = MuleSettings.DefaultLane,
            Payload = "{}",
            PayloadType = "payload",
            Status = status,
            CreatedOnUtc = DateTimeOffset.UtcNow,
            LockedOnUtc = status == DurableActionStatus.Locked ? DateTimeOffset.UtcNow : null,
            CompletedOnUtc = status == DurableActionStatus.Completed ? DateTimeOffset.UtcNow : null,
            TerminalOnUtc = status is DurableActionStatus.Completed or DurableActionStatus.Failed
                ? DateTimeOffset.UtcNow
                : null
        };
}
