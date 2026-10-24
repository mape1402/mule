namespace Mule.Tests;

using System.Text.Json;
using System.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Mule.Dispatching;
using Mule.Diagnostics;
using Mule.Replay;
using NSubstitute;

public sealed class ReplayCoreTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.replay.core.v1");

    [Fact]
    public void ReplayModel_Should_Normalize_And_Merge_Metadata()
    {
        var now = DateTimeOffset.UtcNow;
        var request = MuleReplayModel.NormalizeRequest(new MuleReplayRequest
        {
            RequestId = Ulid.NewUlid(),
            ScheduleOnUtc = now.AddMinutes(-1),
            Metadata = new Dictionary<string, string>
            {
                ["z"] = "last",
                ["a"] = "first"
            }
        }, now);
        var source = new DurableAction
        {
            Key = Key,
            Lane = "critical",
            Payload = "payload",
            PayloadType = "type",
            CorrelationId = "correlation",
            Metadata = "{\"keep\":\"yes\",\"replace\":\"old\"}",
            DeduplicationKey = "original"
        };
        var replay = new MuleReplay
        {
            RequestId = request.RequestId,
            ReplayActionId = Ulid.NewUlid(),
            Metadata = "{\"replace\":\"new\"}"
        };

        var action = MuleReplayModel.CreateAction(new MuleReplayClaim
        {
            Replay = replay,
            SourceAction = source,
            LockToken = Ulid.NewUlid()
        }, now);

        using var metadata = JsonDocument.Parse(action.Metadata);
        Assert.Equal(now, request.ScheduleOnUtc);
        Assert.Equal("first", request.Metadata["a"]);
        Assert.Equal(replay.ReplayActionId, action.Id);
        Assert.Equal(Key, action.Key);
        Assert.Equal("critical", action.Lane);
        Assert.Equal("yes", metadata.RootElement.GetProperty("keep").GetString());
        Assert.Equal("new", metadata.RootElement.GetProperty("replace").GetString());
        Assert.StartsWith("mule:replay:", action.DeduplicationKey);
        Assert.Equal(DurableActionStatus.Pending, action.Status);
    }

    [Fact]
    public void ReplayModel_Should_Preserve_Original_Metadata_When_No_Enrichment_Exists()
    {
        var source = new DurableAction { Key = Key, Metadata = "{\"original\":\"value\"}" };
        var replay = new MuleReplay { RequestId = Ulid.NewUlid(), ReplayActionId = Ulid.NewUlid() };

        var action = MuleReplayModel.CreateAction(new MuleReplayClaim
        {
            Replay = replay,
            SourceAction = source,
            LockToken = Ulid.NewUlid()
        }, DateTimeOffset.UtcNow);

        Assert.Equal(source.Metadata, action.Metadata);
        Assert.Empty(MuleReplayModel.DeserializeMetadata(null));
        Assert.Empty(MuleReplayModel.DeserializeMetadata("null"));
        Assert.Null(MuleReplayModel.SerializeMetadata(null));
        Assert.Null(MuleReplayModel.SerializeMetadata(new Dictionary<string, string>()));
        Assert.Null(MuleReplayModel.ToInfo(null));

        var emptyMerge = MuleReplayModel.CreateAction(new MuleReplayClaim
        {
            Replay = new MuleReplay
            {
                RequestId = Ulid.NewUlid(),
                ReplayActionId = Ulid.NewUlid(),
                Metadata = "{}"
            },
            SourceAction = new DurableAction { Key = Key },
            LockToken = Ulid.NewUlid()
        }, DateTimeOffset.UtcNow);
        Assert.Null(emptyMerge.Metadata);
    }

    [Fact]
    public void ReplayModel_Should_Compare_Every_Idempotency_Field()
    {
        var now = DateTimeOffset.UtcNow;
        var requestId = Ulid.NewUlid();
        var sourceId = Ulid.NewUlid();
        var request = new MuleReplayRequest
        {
            RequestId = requestId,
            RequestedBy = "support",
            Reason = "reason",
            ScheduleOnUtc = now,
            Metadata = new Dictionary<string, string> { ["key"] = "value" },
            AllowParallelReplay = true
        };
        var replay = new MuleReplay
        {
            RequestId = requestId,
            SourceActionId = sourceId,
            RequestedBy = request.RequestedBy,
            Reason = request.Reason,
            ScheduleOnUtc = now,
            Metadata = MuleReplayModel.SerializeMetadata(request.Metadata),
            BranchId = requestId
        };

        Assert.True(MuleReplayModel.Matches(replay, sourceId, request));
        Assert.False(MuleReplayModel.Matches(replay, Ulid.NewUlid(), request));
        Assert.False(MuleReplayModel.Matches(replay, sourceId, Copy(request, requestedBy: "other")));
        Assert.False(MuleReplayModel.Matches(replay, sourceId, Copy(request, reason: "other")));
        Assert.False(MuleReplayModel.Matches(replay, sourceId, Copy(request, scheduleOnUtc: now.AddSeconds(1))));
        Assert.False(MuleReplayModel.Matches(replay, sourceId, Copy(request, metadata: new Dictionary<string, string>())));
        Assert.False(MuleReplayModel.Matches(replay, sourceId, Copy(request, allowParallel: false)));
        Assert.True(MuleReplayModel.Matches(replay, sourceId, Copy(request, scheduleOnUtc: null)));

        var linearRequest = Copy(request, allowParallel: false);
        var linearReplay = new MuleReplay
        {
            RequestId = requestId,
            SourceActionId = sourceId,
            RequestedBy = request.RequestedBy,
            Reason = request.Reason,
            ScheduleOnUtc = now,
            Metadata = MuleReplayModel.SerializeMetadata(request.Metadata),
            BranchId = Ulid.Empty
        };
        Assert.True(MuleReplayModel.Matches(linearReplay, sourceId, linearRequest));
    }

    [Fact]
    public async Task ReplayService_Should_Validate_And_Schedule_Replay()
    {
        var now = DateTimeOffset.UtcNow;
        var sourceId = Ulid.NewUlid();
        var requestId = Ulid.NewUlid();
        var storage = Substitute.For<IMuleReplayStorage>();
        var notifier = new RecordingReplayNotifier();
        var expected = Result(MuleReplayOutcome.Scheduled, sourceId, requestId, now);
        storage.ScheduleAsync(sourceId, Arg.Any<MuleReplayRequest>(), now, Arg.Any<CancellationToken>())
            .Returns(expected);
        storage.FindAsync(requestId, Arg.Any<CancellationToken>()).Returns(expected.Replay);
        var service = new MuleReplayService(
            storage,
            notifier,
            new FixedTimeProvider(now),
            new MuleRuntimeMetrics(),
            NullLogger<MuleReplayService>.Instance);

        var result = await service.ReplayAsync(sourceId, new MuleReplayRequest
        {
            RequestId = requestId,
            ScheduleOnUtc = now.AddMinutes(-1)
        });

        Assert.Same(expected, result);
        Assert.Same(expected.Replay, await service.GetReplayAsync(requestId));
        Assert.Equal([now], notifier.Schedules);
        await Assert.ThrowsAsync<ArgumentException>(async () => await service.ReplayAsync(Ulid.Empty, new MuleReplayRequest { RequestId = requestId }));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await service.ReplayAsync(sourceId, null));
        await Assert.ThrowsAsync<ArgumentException>(async () => await service.ReplayAsync(sourceId, new MuleReplayRequest { RequestId = Ulid.Empty }));
        await Assert.ThrowsAsync<ArgumentException>(async () => await service.GetReplayAsync(Ulid.Empty));
    }

    [Theory]
    [InlineData(MuleReplayOutcome.AlreadyScheduled)]
    [InlineData(MuleReplayOutcome.NotFound)]
    [InlineData(MuleReplayOutcome.NotEligible)]
    [InlineData(MuleReplayOutcome.RequestConflict)]
    [InlineData(MuleReplayOutcome.ReplayAlreadyExists)]
    public async Task ReplayService_Should_Return_NonCreated_Outcomes_Without_Signaling(MuleReplayOutcome outcome)
    {
        var now = DateTimeOffset.UtcNow;
        var sourceId = Ulid.NewUlid();
        var requestId = Ulid.NewUlid();
        var storage = Substitute.For<IMuleReplayStorage>();
        var notifier = new RecordingReplayNotifier();
        storage.ScheduleAsync(sourceId, Arg.Any<MuleReplayRequest>(), now, Arg.Any<CancellationToken>())
            .Returns(Result(outcome, sourceId, requestId, now));
        var service = new MuleReplayService(storage, notifier, new FixedTimeProvider(now), new MuleRuntimeMetrics(), NullLogger<MuleReplayService>.Instance);

        var result = await service.ReplayAsync(sourceId, new MuleReplayRequest { RequestId = requestId });

        Assert.Equal(outcome, result.Outcome);
        Assert.Empty(notifier.Schedules);
    }

    [Fact]
    public async Task ReplayService_Should_Log_And_Propagate_Storage_Failures()
    {
        var storage = Substitute.For<IMuleReplayStorage>();
        storage.ScheduleAsync(Arg.Any<Ulid>(), Arg.Any<MuleReplayRequest>(), Arg.Any<DateTimeOffset>(), Arg.Any<CancellationToken>())
            .Returns<Task<MuleReplayResult>>(_ => throw new InvalidOperationException("storage failed"));
        var service = new MuleReplayService(
            storage,
            new RecordingReplayNotifier(),
            TimeProvider.System,
            new MuleRuntimeMetrics(),
            NullLogger<MuleReplayService>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await service.ReplayAsync(
            Ulid.NewUlid(),
            new MuleReplayRequest { RequestId = Ulid.NewUlid() }));
    }

    [Fact]
    public async Task ReplayService_Should_Handle_Rejected_Result_Without_Replay_Details()
    {
        var now = DateTimeOffset.UtcNow;
        var storage = Substitute.For<IMuleReplayStorage>();
        storage.ScheduleAsync(Arg.Any<Ulid>(), Arg.Any<MuleReplayRequest>(), now, Arg.Any<CancellationToken>())
            .Returns(new MuleReplayResult { Outcome = MuleReplayOutcome.NotFound });
        var service = new MuleReplayService(
            storage,
            new RecordingReplayNotifier(),
            new FixedTimeProvider(now),
            new MuleRuntimeMetrics(),
            NullLogger<MuleReplayService>.Instance);

        var result = await service.ReplayAsync(
            Ulid.NewUlid(),
            new MuleReplayRequest { RequestId = Ulid.NewUlid() });

        Assert.Equal(MuleReplayOutcome.NotFound, result.Outcome);
        Assert.Equal(Ulid.Empty, result.OriginalActionId);
        Assert.Equal(Ulid.Empty, result.ReplayActionId);
    }

    [Fact]
    public void ReplayService_Should_Validate_Dependencies_And_Empty_Result()
    {
        var storage = Substitute.For<IMuleReplayStorage>();
        var notifier = new RecordingReplayNotifier();
        var time = TimeProvider.System;
        var metrics = new MuleRuntimeMetrics();
        var logger = NullLogger<MuleReplayService>.Instance;

        Assert.Throws<ArgumentNullException>(() => new MuleReplayService(null, notifier, time, metrics, logger));
        Assert.Throws<ArgumentNullException>(() => new MuleReplayService(storage, null, time, metrics, logger));
        Assert.Throws<ArgumentNullException>(() => new MuleReplayService(storage, notifier, null, metrics, logger));
        Assert.Throws<ArgumentNullException>(() => new MuleReplayService(storage, notifier, time, null, logger));
        Assert.Throws<ArgumentNullException>(() => new MuleReplayService(storage, notifier, time, metrics, null));

        var result = new MuleReplayResult { Outcome = MuleReplayOutcome.NotFound };
        Assert.Equal(Ulid.Empty, result.OriginalActionId);
        Assert.Equal(Ulid.Empty, result.ReplayActionId);
    }

    [Fact]
    public void ReplayResult_Should_Expose_NonEmpty_Action_Ids()
    {
        var sourceId = Ulid.NewUlid();
        var replayId = Ulid.NewUlid();
        var result = new MuleReplayResult
        {
            Outcome = MuleReplayOutcome.Scheduled,
            Replay = new MuleReplayInfo
            {
                RequestId = Ulid.NewUlid(),
                ReplayOfActionId = sourceId,
                ReplayRootActionId = sourceId,
                ReplayActionId = replayId,
                ReplaySequence = 1,
                Status = MuleReplayStatus.Scheduled,
                ActionKey = Key,
                RequestedOnUtc = DateTimeOffset.UtcNow,
                ScheduleOnUtc = DateTimeOffset.UtcNow
            }
        };

        Assert.Equal(sourceId, result.OriginalActionId);
        Assert.Equal(replayId, result.ReplayActionId);
    }

    [Fact]
    public async Task ReplayCommitNotifier_Should_Not_Signal_Rolled_Back_Transaction()
    {
        var signal = new MuleSchedulerSignal();
        var notifier = new AmbientTransactionMuleReplayCommitNotifier(signal);
        Assert.Throws<ArgumentNullException>(() => new AmbientTransactionMuleReplayCommitNotifier(null));

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            await notifier.NotifyScheduledAsync(DateTimeOffset.UtcNow);

        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await signal.WaitForReplayAsync(timeout.Token));
    }

    [Fact]
    public async Task ReplayCommitNotifier_Should_Signal_Only_After_Commit()
    {
        var signal = new MuleSchedulerSignal();
        var notifier = new AmbientTransactionMuleReplayCommitNotifier(signal);
        var due = DateTimeOffset.UtcNow;

        await notifier.NotifyScheduledAsync(due);
        await signal.WaitForReplayAsync(CancellationToken.None);

        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await notifier.NotifyScheduledAsync(due);
            scope.Complete();
        }

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await signal.WaitForReplayAsync(timeout.Token);
    }

    [Fact]
    public void AddMule_Should_Register_Replay_Services_And_Defaults()
    {
        var services = new ServiceCollection();
        services.AddMule();
        var settings = new MuleSettings().Replay;

        Assert.Contains(services, x => x.ServiceType == typeof(IMuleReplayService));
        Assert.Contains(services, x => x.ServiceType == typeof(TimeProvider));
        Assert.Equal(MuleRecoveryMode.Scheduled, settings.RecoveryMode);
        Assert.Equal(50, settings.BatchSize);
        Assert.Equal(1, settings.WorkerCount);
        Assert.Equal(TimeSpan.FromMinutes(1), settings.LockTimeout);
        Assert.Equal(TimeSpan.FromSeconds(10), settings.RetryDelay);
    }

    [Fact]
    public void RuntimeMetrics_Should_Expose_Replay_Counters()
    {
        var metrics = new MuleRuntimeMetrics();
        metrics.RecordReplayScheduled();
        metrics.RecordReplayMaterialized();
        metrics.RecordReplayAlreadyScheduled();
        metrics.RecordReplayRejected();
        metrics.RecordReplayRetried();
        metrics.RecordReplayMaterializationFailure();

        var snapshot = metrics.GetSnapshot(DateTimeOffset.UtcNow);

        Assert.Equal(1, snapshot.ReplayScheduled);
        Assert.Equal(1, snapshot.ReplayMaterialized);
        Assert.Equal(1, snapshot.ReplayAlreadyScheduled);
        Assert.Equal(1, snapshot.ReplayRejected);
        Assert.Equal(1, snapshot.ReplayRetried);
        Assert.Equal(1, snapshot.ReplayMaterializationFailures);
    }

    private static MuleReplayResult Result(
        MuleReplayOutcome outcome,
        Ulid sourceId,
        Ulid requestId,
        DateTimeOffset now)
        => new()
        {
            Outcome = outcome,
            Replay = new MuleReplayInfo
            {
                RequestId = requestId,
                ReplayOfActionId = sourceId,
                ReplayRootActionId = sourceId,
                ReplayActionId = Ulid.NewUlid(),
                ReplaySequence = 1,
                Status = MuleReplayStatus.Scheduled,
                ActionKey = Key,
                Lane = MuleSettings.DefaultLane,
                RequestedOnUtc = now,
                ScheduleOnUtc = now
            }
        };

    private static MuleReplayRequest Copy(
        MuleReplayRequest source,
        string requestedBy = null,
        string reason = null,
        DateTimeOffset? scheduleOnUtc = null,
        IReadOnlyDictionary<string, string> metadata = null,
        bool? allowParallel = null)
        => new()
        {
            RequestId = source.RequestId,
            RequestedBy = requestedBy ?? source.RequestedBy,
            Reason = reason ?? source.Reason,
            ScheduleOnUtc = scheduleOnUtc,
            Metadata = metadata ?? source.Metadata,
            AllowParallelReplay = allowParallel ?? source.AllowParallelReplay
        };

    private sealed class FixedTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _now;

        public FixedTimeProvider(DateTimeOffset now)
            => _now = now;

        public override DateTimeOffset GetUtcNow() => _now;
    }

    private sealed class RecordingReplayNotifier : IMuleReplayCommitNotifier
    {
        public List<DateTimeOffset> Schedules { get; } = new();

        public ValueTask NotifyScheduledAsync(DateTimeOffset scheduleOnUtc)
        {
            Schedules.Add(scheduleOnUtc);
            return ValueTask.CompletedTask;
        }
    }
}
