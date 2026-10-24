namespace Mule.FastLane.InMemory.Tests;

using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

public sealed class FastLaneInMemoryCoverageTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.fastlane.inmemory.coverage");

    [Fact]
    public void Buffer_Should_Handle_Lifecycle_And_Remove_After_Both_Flushes()
    {
        var buffer = new InMemoryFastLaneBuffer();
        var now = DateTimeOffset.UtcNow;
        var action = CreateAction("one", now.AddSeconds(-3));
        var duplicate = CreateAction("dup", now, deduplicationKey: action.DeduplicationKey);
        var future = CreateAction("future", now, nextAttemptOnUtc: now.AddMinutes(5));
        var expired = CreateAction("expired", now, DurableActionStatus.Locked, lockedOnUtc: now.AddMinutes(-10));
        var active = CreateAction("active", now, DurableActionStatus.Locked, lockedOnUtc: now);
        var otherLane = CreateAction("other-lane", now.AddSeconds(-1));
        otherLane.Lane = "other";
        var defaultLane = CreateAction("default-lane", now.AddSeconds(-2));
        defaultLane.Lane = " ";
        defaultLane.DeduplicationKey = null;
        var dueWithDate = CreateAction("due-with-date", now.AddSeconds(-2), nextAttemptOnUtc: now.AddSeconds(-1));
        var lockedWithoutDate = CreateAction("locked-null", now, DurableActionStatus.Locked);

        Assert.Throws<ArgumentNullException>(() => buffer.TryAdd(null));
        Assert.True(buffer.TryAdd(action));
        Assert.False(buffer.TryAdd(duplicate));
        Assert.True(buffer.TryAdd(future));
        Assert.True(buffer.TryAdd(expired));
        Assert.True(buffer.TryAdd(active));
        Assert.True(buffer.TryAdd(otherLane));
        Assert.True(buffer.TryAdd(defaultLane));
        Assert.True(buffer.TryAdd(dueWithDate));
        Assert.True(buffer.TryAdd(lockedWithoutDate));
        Assert.True(buffer.TryFindByDeduplicationKey(Key, action.DeduplicationKey, out var dedupId));
        Assert.Equal(action.Id, dedupId);
        Assert.False(buffer.TryFindByDeduplicationKey(Key, "missing", out _));
        Assert.Null(buffer.Lock(Ulid.NewUlid(), TimeSpan.FromMinutes(1), now));
        Assert.NotNull(buffer.GetNextPendingOnUtc(TimeSpan.FromMinutes(1), now));

        var claimed = buffer.ClaimPending("Lane-A", 10, TimeSpan.FromMinutes(1), now).ToArray();

        Assert.Contains(claimed, x => x.Id == action.Id);
        Assert.Contains(claimed, x => x.Id == expired.Id);
        Assert.DoesNotContain(claimed, x => x.Id == future.Id);
        Assert.NotNull(buffer.GetNextPendingOnUtc(TimeSpan.FromMinutes(1), now));
        Assert.Null(buffer.Lock(action.Id, TimeSpan.FromMinutes(5), now));
        Assert.Null(buffer.Lock(active.Id, TimeSpan.FromMinutes(5), now));
        Assert.NotNull(buffer.Lock(defaultLane.Id, TimeSpan.FromMinutes(1), now));

        var intents = buffer.TakeIntentFlushBatch(10).ToArray();
        Assert.NotEmpty(intents);
        buffer.CancelIntentFlush([action.Id, Ulid.NewUlid()]);
        intents = buffer.TakeIntentFlushBatch(10).ToArray();
        buffer.CompleteIntentFlush(intents.Select(x => x.Id).Append(Ulid.NewUlid()).ToArray());

        Assert.True(buffer.MarkCompleted(action.Id, now.AddSeconds(1)));
        Assert.True(buffer.MarkFailed(expired.Id, "terminal", now.AddSeconds(2), null));
        Assert.True(buffer.MarkFailed(future.Id, "retry", now.AddSeconds(3), now.AddSeconds(4)));
        Assert.False(buffer.MarkCompleted(Ulid.NewUlid(), now));
        Assert.False(buffer.MarkFailed(Ulid.NewUlid(), "missing", now, null));

        var terminal = buffer.TakeTerminalFlushBatch(10).ToArray();
        Assert.Contains(terminal, x => x.Id == action.Id);
        Assert.Empty(buffer.TakeTerminalFlushBatch(10));
        buffer.CancelTerminalFlush([action.Id, Ulid.NewUlid()]);
        terminal = buffer.TakeTerminalFlushBatch(10).ToArray();
        buffer.CompleteTerminalFlush(terminal.Select(x => x.Id).Append(Ulid.NewUlid()).ToArray());

        Assert.False(buffer.Contains(action.Id));

        var completedBeforeIntent = CreateAction("completed-before-intent", now, DurableActionStatus.Completed);
        completedBeforeIntent.Attempts = 3;
        var failedBeforeIntent = CreateAction("failed-before-intent", now, DurableActionStatus.Failed);
        failedBeforeIntent.Attempts = 3;
        Assert.True(buffer.TryAdd(completedBeforeIntent));
        Assert.True(buffer.TryAdd(failedBeforeIntent));
        var durableIntents = buffer.TakeIntentFlushBatch(10);
        Assert.Contains(durableIntents, x => x.Id == completedBeforeIntent.Id && x.Status == DurableActionStatus.Locked);
        Assert.Contains(durableIntents, x => x.Id == failedBeforeIntent.Id && x.Attempts == 2);
    }

    [Fact]
    public async Task Storage_Should_Buffer_New_Actions_And_Fallback_To_Durable_Storage()
    {
        var durable = new RecordingDurableStorage();
        var buffer = new InMemoryFastLaneBuffer();
        var storage = new FastLaneInMemoryStorage(durable, buffer);
        var now = DateTimeOffset.UtcNow;
        var buffered = CreateAction("buffered", now);
        var durableOnly = CreateAction("durable", now);
        durable.DeduplicatedId = durableOnly.Id;
        durable.NextPending = now;
        durable.Pending.Add(durableOnly);

        await Assert.ThrowsAsync<ArgumentNullException>(() => storage.AddAsync(null));
        await Assert.ThrowsAsync<ArgumentNullException>(() => storage.AddRangeAsync(null));
        await Assert.ThrowsAsync<ArgumentException>(() => storage.AddRangeAsync([null]));

        await storage.AddAsync(buffered);
        await storage.AddRangeAsync([CreateAction("skip", now, deduplicationKey: "durable-dedupe")]);
        Assert.Equal(now, await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now));
        await storage.SaveChangesAsync();

        durable.NextPending = now.AddMinutes(1);
        Assert.True(await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now) < durable.NextPending);
        durable.NextPending = now;

        Assert.True(buffer.Contains(buffered.Id));
        Assert.True(durable.Saved);
        Assert.Equal(buffered.Id, await storage.FindByDeduplicationKeyAsync(Key, buffered.DeduplicationKey));
        Assert.Null(await storage.FindByDeduplicationKeyAsync(Key, ""));
        Assert.Equal(durable.DeduplicatedId, await storage.FindByDeduplicationKeyAsync(Key, "durable-dedupe"));
        durable.NextPending = null;
        Assert.NotNull(await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now));
        durable.NextPending = now;

        var claimed = await storage.ClaimPendingAsync("Lane-A", 5, TimeSpan.FromMinutes(1), now);
        Assert.Contains(claimed, x => x.Id == buffered.Id);
        Assert.Contains(claimed, x => x.Id == durableOnly.Id);
        Assert.Equal(buffered.Id, (await storage.LockAsync(buffered.Id, TimeSpan.FromMinutes(1), now.AddMinutes(2))).Id);
        Assert.Equal(durableOnly.Id, (await storage.LockAsync(durableOnly.Id, TimeSpan.FromMinutes(5), now)).Id);

        var shortCircuitBuffer = new InMemoryFastLaneBuffer();
        var shortCircuitStorage = new FastLaneInMemoryStorage(durable, shortCircuitBuffer);
        var shortCircuitAction = CreateAction("short-circuit", now.AddSeconds(-1));
        await shortCircuitStorage.AddAsync(shortCircuitAction);
        await shortCircuitStorage.SaveChangesAsync();
        Assert.Single(await shortCircuitStorage.ClaimPendingAsync("Lane-A", 1, TimeSpan.FromMinutes(1), now));

        var next = await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now);
        Assert.NotNull(next);
        durable.NextPending = now.AddMinutes(-1);
        Assert.Equal(durable.NextPending, await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now));
        await storage.MarkCompletedAsync(buffered.Id, now);
        await storage.MarkCompletedAsync(durableOnly.Id, now);
        await storage.MarkFailedAsync(buffered.Id, "buffered", now, null);
        await storage.MarkFailedAsync(durableOnly.Id, "durable", now, null);
        Assert.Equal(1, await storage.CleanCompletedAsync(now, 10));
        Assert.NotNull(await storage.GetOldestCompletedOnUtcAsync());
        await storage.SaveChangesAsync();

        Assert.True(durable.MarkCompletedCalled);
        Assert.True(durable.MarkFailedCalled);
    }

    [Fact]
    public async Task Cancellation_Should_Cover_Buffer_Outcomes_And_Durable_Fallbacks()
    {
        var now = DateTimeOffset.UtcNow;
        var buffer = new InMemoryFastLaneBuffer();
        var pending = CreateAction("cancel-pending", now);
        var locked = CreateAction("cancel-locked", now, DurableActionStatus.Locked, now);
        Assert.True(buffer.TryAdd(pending));
        Assert.True(buffer.TryAdd(locked));

        Assert.False(buffer.TryCancel(Ulid.NewUlid(), now, out var missing));
        Assert.Null(missing);
        Assert.True(buffer.TryCancel(pending.Id, now, out var cancelled));
        Assert.Equal(MuleCancellationOutcome.Cancelled, cancelled.Outcome);
        Assert.True(buffer.TryCancel(pending.Id, now.AddMinutes(1), out var repeated));
        Assert.Equal(MuleCancellationOutcome.AlreadyCancelled, repeated.Outcome);
        Assert.Equal(now, repeated.CancelledOnUtc);
        Assert.True(buffer.TryCancel(locked.Id, now, out var tooLate));
        Assert.Equal(MuleCancellationOutcome.TooLate, tooLate.Outcome);
        Assert.Contains(
            buffer.TakeIntentFlushBatch(10),
            x => x.Id == pending.Id && x.Status == DurableActionStatus.Cancelled);

        var unsupported = Substitute.For<IMuleDurableStorage>();
        var unsupportedStorage = new FastLaneInMemoryStorage(unsupported, new InMemoryFastLaneBuffer());
        Assert.Equal(
            MuleCancellationOutcome.NotSupported,
            (await unsupportedStorage.CancelAsync(Ulid.NewUlid(), now)).Outcome);

        var supported = Substitute.For<IMuleDurableStorage, IMuleCancellationStorage>();
        var expected = new MuleCancellationResult(Ulid.NewUlid(), MuleCancellationOutcome.Cancelled, now);
        ((IMuleCancellationStorage)supported)
            .CancelAsync(expected.ActionId, now, Arg.Any<CancellationToken>())
            .Returns(expected);
        var supportedStorage = new FastLaneInMemoryStorage(supported, new InMemoryFastLaneBuffer());
        Assert.Same(expected, await supportedStorage.CancelAsync(expected.ActionId, now));
    }

    [Fact]
    public async Task GetNextPendingOnUtcAsync_Should_Return_Earlier_Durable_Due_Time()
    {
        var now = DateTimeOffset.UtcNow;
        var durable = new RecordingDurableStorage
        {
            NextPending = now.AddMinutes(-1)
        };
        var storage = new FastLaneInMemoryStorage(durable, new InMemoryFastLaneBuffer());

        await storage.AddAsync(CreateAction("buffered-later", now));
        await storage.SaveChangesAsync();

        Assert.Equal(durable.NextPending, await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now));

        durable.NextPending = now.AddMinutes(1);
        Assert.Equal(now, await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now));
    }

    [Fact]
    public async Task FlushService_Should_Complete_And_Cancel_Flushes()
    {
        var durable = new RecordingDurableStorage();
        var buffer = new InMemoryFastLaneBuffer();
        var service = CreateService(buffer, durable);
        var now = DateTimeOffset.UtcNow;
        var action = CreateAction("flush", now);

        buffer.TryAdd(action);
        await InvokeFlushOnceAsync(service);
        Assert.Single(durable.Added);

        buffer.MarkCompleted(action.Id, now);
        await InvokeFlushOnceAsync(service);
        Assert.True(durable.MarkCompletedCalled);
        Assert.False(buffer.Contains(action.Id));

        var nullCompleted = CreateAction("null-completed-date", now);
        var nullFailed = CreateAction("null-terminal-date", now);
        buffer.TryAdd(nullCompleted);
        buffer.TryAdd(nullFailed);
        await InvokeFlushOnceAsync(service);
        buffer.MarkCompleted(nullCompleted.Id, now);
        buffer.MarkFailed(nullFailed.Id, "failed", now, null);
        GetBufferedAction(buffer, nullCompleted.Id).CompletedOnUtc = null;
        GetBufferedAction(buffer, nullFailed.Id).TerminalOnUtc = null;
        await InvokeFlushOnceAsync(service);

        var failingDurable = new RecordingDurableStorage { ThrowOnAdd = true };
        var failingBuffer = new InMemoryFastLaneBuffer();
        var failingService = CreateService(failingBuffer, failingDurable);
        var failingAction = CreateAction("fail", now);
        failingBuffer.TryAdd(failingAction);

        await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeFlushOnceAsync(failingService));
        Assert.True(failingBuffer.Contains(failingAction.Id));

        failingDurable.ThrowOnAdd = false;
        await InvokeFlushOnceAsync(failingService);
        failingBuffer.MarkFailed(failingAction.Id, "terminal", now, null);
        failingDurable.ThrowOnMarkFailed = true;
        await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeFlushOnceAsync(failingService));

        using var running = CreateService(buffer, durable, TimeSpan.Zero);
        await running.StartAsync(CancellationToken.None);
        await running.StopAsync(CancellationToken.None);

        var loopingBuffer = new InMemoryFastLaneBuffer();
        loopingBuffer.TryAdd(CreateAction("loop-failure", now));
        var loopingDurable = new RecordingDurableStorage { ThrowOnAdd = true };
        using var failingLoop = CreateService(loopingBuffer, loopingDurable, TimeSpan.FromMilliseconds(1));
        await failingLoop.StartAsync(CancellationToken.None);
        await Task.Delay(20);
        await failingLoop.StopAsync(CancellationToken.None);

        var blockingBuffer = new InMemoryFastLaneBuffer();
        blockingBuffer.TryAdd(CreateAction("loop-cancellation", now));
        var blockingDurable = new RecordingDurableStorage { BlockOnAdd = true };
        using var canceledLoop = CreateService(blockingBuffer, blockingDurable, TimeSpan.FromSeconds(1));
        await canceledLoop.StartAsync(CancellationToken.None);
        await Task.Delay(20);
        await canceledLoop.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task FlushService_Should_Reject_Unsupported_Or_Rejected_Cancellation()
    {
        var now = DateTimeOffset.UtcNow;
        var unsupported = Substitute.For<IMuleDurableStorage>();
        var unsupportedBuffer = new InMemoryFastLaneBuffer();
        var unsupportedAction = CreateAction("unsupported-cancel", now);
        unsupportedBuffer.TryAdd(unsupportedAction);
        var unsupportedService = CreateService(unsupportedBuffer, unsupported);
        await InvokeFlushOnceAsync(unsupportedService);
        unsupportedBuffer.TryCancel(unsupportedAction.Id, now, out _);

        await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeFlushOnceAsync(unsupportedService));

        var rejecting = Substitute.For<IMuleDurableStorage, IMuleTerminalCancellationStorage>();
        ((IMuleTerminalCancellationStorage)rejecting)
            .MarkCancelledAsync(Arg.Any<Ulid>(), Arg.Any<DateTimeOffset>(), Arg.Any<CancellationToken>())
            .Returns(false);
        var rejectingBuffer = new InMemoryFastLaneBuffer();
        var rejectingAction = CreateAction("rejected-cancel", now);
        rejectingBuffer.TryAdd(rejectingAction);
        var rejectingService = CreateService(rejectingBuffer, rejecting);
        await InvokeFlushOnceAsync(rejectingService);
        rejectingBuffer.TryCancel(rejectingAction.Id, now, out _);
        GetBufferedAction(rejectingBuffer, rejectingAction.Id).CancelledOnUtc = null;

        await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeFlushOnceAsync(rejectingService));
    }

    [Fact]
    public void Registration_And_Options_Should_Validate_Nulls()
    {
        Assert.Throws<ArgumentNullException>(() => new FastLaneInMemoryStorage(null, new InMemoryFastLaneBuffer()));
        Assert.Throws<ArgumentNullException>(() => new FastLaneInMemoryStorage(new RecordingDurableStorage(), null));
        Assert.Throws<ArgumentNullException>(() => MuleFastLaneInMemoryRegistrationBuilderExtensions.UseFastLaneInMemory(null, _ => { }));
        Assert.Throws<ArgumentNullException>(() => new FastLaneInMemoryFlushService(null, new InMemoryFastLaneBuffer(), Options.Create(new FastLaneInMemoryOptions()), NullLogger<FastLaneInMemoryFlushService>.Instance));
        Assert.Throws<ArgumentNullException>(() => new FastLaneInMemoryFlushService(new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(), null, Options.Create(new FastLaneInMemoryOptions()), NullLogger<FastLaneInMemoryFlushService>.Instance));
        Assert.Throws<ArgumentNullException>(() => new FastLaneInMemoryFlushService(new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(), new InMemoryFastLaneBuffer(), null, NullLogger<FastLaneInMemoryFlushService>.Instance));
        Assert.Throws<ArgumentNullException>(() => new FastLaneInMemoryFlushService(new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(), new InMemoryFastLaneBuffer(), Options.Create(new FastLaneInMemoryOptions()), null));

        var options = new FastLaneInMemoryOptions
        {
            IntentFlushSize = 0,
            CompletionFlushSize = 0,
            FlushInterval = TimeSpan.Zero
        };

        Assert.Equal(0, options.IntentFlushSize);
        Assert.Equal(0, options.CompletionFlushSize);
        Assert.Equal(TimeSpan.Zero, options.FlushInterval);
        Assert.Equal(FastLaneAcknowledge.AfterBufferWrite, options.Acknowledge);
    }

    [Fact]
    public void Buffer_Helpers_Should_Cover_Every_Lock_State_And_Lane_Normalization()
    {
        var now = DateTimeOffset.UtcNow;
        Assert.True(InvokeCanLock(CreateAction("pending-null", now), now));
        Assert.True(InvokeCanLock(CreateAction("pending-due", now, nextAttemptOnUtc: now.AddSeconds(-1)), now));
        Assert.False(InvokeCanLock(CreateAction("pending-future", now, nextAttemptOnUtc: now.AddSeconds(1)), now));
        Assert.True(InvokeCanLock(CreateAction("locked-expired", now, DurableActionStatus.Locked, now.AddMinutes(-2)), now));
        Assert.False(InvokeCanLock(CreateAction("locked-active", now, DurableActionStatus.Locked, now), now));
        Assert.False(InvokeCanLock(CreateAction("locked-null", now, DurableActionStatus.Locked), now));
        Assert.False(InvokeCanLock(CreateAction("completed", now, DurableActionStatus.Completed), now));

        var normalize = typeof(InMemoryFastLaneBuffer).GetMethod("NormalizeLane", BindingFlags.Static | BindingFlags.NonPublic);
        Assert.Equal(MuleSettings.DefaultLane, normalize.Invoke(null, [" "]));
        Assert.Equal("lane", normalize.Invoke(null, ["lane"]));

        var terminalMatrix = new InMemoryFastLaneBuffer();
        var matrixAction = CreateAction("matrix", now);
        terminalMatrix.TryAdd(matrixAction);
        Assert.Empty(terminalMatrix.TakeTerminalFlushBatch(1));
        terminalMatrix.CompleteIntentFlush([matrixAction.Id]);
        Assert.Empty(terminalMatrix.TakeTerminalFlushBatch(1));
        terminalMatrix.MarkCompleted(matrixAction.Id, now);
        GetBufferedAction(terminalMatrix, matrixAction.Id).TerminalOnUtc = null;
        Assert.Single(terminalMatrix.TakeTerminalFlushBatch(1));
        Assert.Empty(terminalMatrix.TakeTerminalFlushBatch(1));
    }

    private static bool InvokeCanLock(DurableAction action, DateTimeOffset now)
    {
        var method = typeof(InMemoryFastLaneBuffer).GetMethod("CanLock", BindingFlags.Static | BindingFlags.NonPublic);
        return (bool)method.Invoke(null, [action, TimeSpan.FromMinutes(1), now]);
    }

    private static DurableAction GetBufferedAction(InMemoryFastLaneBuffer buffer, Ulid id)
    {
        var actions = typeof(InMemoryFastLaneBuffer).GetField("_actions", BindingFlags.Instance | BindingFlags.NonPublic)
            .GetValue(buffer);
        var arguments = new object[] { id, null };
        actions.GetType().GetMethod("TryGetValue").Invoke(actions, arguments);
        return (DurableAction)arguments[1].GetType().GetProperty("Action").GetValue(arguments[1]);
    }

    private static FastLaneInMemoryFlushService CreateService(
        InMemoryFastLaneBuffer buffer,
        IMuleDurableStorage durable,
        TimeSpan? flushInterval = null)
    {
        var services = new ServiceCollection()
            .AddSingleton<IMuleDurableStorage>(durable)
            .BuildServiceProvider();

        return new FastLaneInMemoryFlushService(
            services.GetRequiredService<IServiceScopeFactory>(),
            buffer,
            Options.Create(new FastLaneInMemoryOptions
            {
                IntentFlushSize = 10,
                CompletionFlushSize = 10,
                FlushInterval = flushInterval ?? TimeSpan.Zero
            }),
            NullLogger<FastLaneInMemoryFlushService>.Instance);
    }

    private static async Task InvokeFlushOnceAsync(FastLaneInMemoryFlushService service)
    {
        var method = typeof(FastLaneInMemoryFlushService).GetMethod(
            "FlushOnceAsync",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var task = (Task)method.Invoke(service, [CancellationToken.None]);
        await task;
    }

    private static DurableAction CreateAction(
        string value,
        DateTimeOffset createdOnUtc,
        DurableActionStatus status = DurableActionStatus.Pending,
        DateTimeOffset? lockedOnUtc = null,
        DateTimeOffset? nextAttemptOnUtc = null,
        string deduplicationKey = null)
        => new()
        {
            Key = Key,
            Lane = "Lane-A",
            Payload = $"\"{value}\"",
            PayloadType = typeof(string).AssemblyQualifiedName,
            DeduplicationKey = deduplicationKey ?? value,
            Status = status,
            CreatedOnUtc = createdOnUtc,
            LockedOnUtc = lockedOnUtc,
            StartedOnUtc = lockedOnUtc,
            NextAttemptOnUtc = nextAttemptOnUtc,
            Attempts = status == DurableActionStatus.Failed ? 1 : 0,
            LastError = status == DurableActionStatus.Failed ? "failed" : null
        };

    private sealed class RecordingDurableStorage : IMuleDurableStorage
    {
        public List<DurableAction> Added { get; } = new();
        public List<DurableAction> Pending { get; } = new();
        public Ulid? DeduplicatedId { get; set; }
        public DateTimeOffset? NextPending { get; set; } = DateTimeOffset.UtcNow;
        public bool Saved { get; private set; }
        public bool MarkCompletedCalled { get; private set; }
        public bool MarkFailedCalled { get; private set; }
        public bool ThrowOnAdd { get; set; }
        public bool BlockOnAdd { get; set; }
        public bool ThrowOnMarkFailed { get; set; }

        public Task AddAsync(DurableAction action, CancellationToken cancellationToken = default)
        {
            Added.Add(action);
            return Task.CompletedTask;
        }

        public async Task AddRangeAsync(IReadOnlyCollection<DurableAction> actions, CancellationToken cancellationToken = default)
        {
            if (BlockOnAdd)
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);

            if (ThrowOnAdd)
                throw new InvalidOperationException("add failed");

            Added.AddRange(actions);
        }

        public Task<Ulid?> FindByDeduplicationKeyAsync(ActionKey key, string deduplicationKey, CancellationToken cancellationToken = default)
            => Task.FromResult(deduplicationKey == "durable-dedupe" ? DeduplicatedId : null);

        public Task<IReadOnlyCollection<DurableAction>> ClaimPendingAsync(string lane, int batchSize, TimeSpan lockTimeout, DateTimeOffset now, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyCollection<DurableAction>>(Pending.Take(batchSize).ToArray());

        public Task<DateTimeOffset?> GetNextPendingOnUtcAsync(TimeSpan lockTimeout, DateTimeOffset now, CancellationToken cancellationToken = default)
            => Task.FromResult(NextPending);

        public Task<DurableAction> LockAsync(Ulid id, TimeSpan lockTimeout, DateTimeOffset now, CancellationToken cancellationToken = default)
            => Task.FromResult(Pending.SingleOrDefault(x => x.Id == id));

        public Task MarkCompletedAsync(Ulid id, DateTimeOffset completedOnUtc, CancellationToken cancellationToken = default)
        {
            MarkCompletedCalled = true;
            return Task.CompletedTask;
        }

        public Task MarkFailedAsync(Ulid id, string error, DateTimeOffset now, DateTimeOffset? nextAttemptOnUtc, CancellationToken cancellationToken = default)
        {
            if (ThrowOnMarkFailed)
                throw new InvalidOperationException("mark failed");

            MarkFailedCalled = true;
            return Task.CompletedTask;
        }

        public Task<int> CleanCompletedAsync(DateTimeOffset olderThanUtc, int batchSize, CancellationToken cancellationToken = default)
            => Task.FromResult(1);

        public Task<DateTimeOffset?> GetOldestCompletedOnUtcAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<DateTimeOffset?>(DateTimeOffset.UtcNow);

        public Task SaveChangesAsync(CancellationToken cancellationToken = default)
        {
            Saved = true;
            return Task.CompletedTask;
        }
    }
}
