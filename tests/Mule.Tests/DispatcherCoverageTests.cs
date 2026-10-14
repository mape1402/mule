namespace Mule.Tests;

using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Mule.Diagnostics;
using Mule.Dispatching;

public sealed class DispatcherCoverageTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.dispatcher.coverage");

    [Fact]
    public void Constructor_Should_Validate_Every_Dependency()
    {
        var fixture = new DispatcherFixture(new MuleSettings());

        Assert.Throws<ArgumentNullException>(() => new MuleDispatcherHostedService(null, fixture.Queue, fixture.Executor, fixture.Signal, fixture.Settings, fixture.Metrics, fixture.Logger));
        Assert.Throws<ArgumentNullException>(() => new MuleDispatcherHostedService(fixture.ScopeFactory, null, fixture.Executor, fixture.Signal, fixture.Settings, fixture.Metrics, fixture.Logger));
        Assert.Throws<ArgumentNullException>(() => new MuleDispatcherHostedService(fixture.ScopeFactory, fixture.Queue, null, fixture.Signal, fixture.Settings, fixture.Metrics, fixture.Logger));
        Assert.Throws<ArgumentNullException>(() => new MuleDispatcherHostedService(fixture.ScopeFactory, fixture.Queue, fixture.Executor, null, fixture.Settings, fixture.Metrics, fixture.Logger));
        Assert.Throws<ArgumentNullException>(() => new MuleDispatcherHostedService(fixture.ScopeFactory, fixture.Queue, fixture.Executor, fixture.Signal, null, fixture.Metrics, fixture.Logger));
        Assert.Throws<ArgumentNullException>(() => new MuleDispatcherHostedService(fixture.ScopeFactory, fixture.Queue, fixture.Executor, fixture.Signal, fixture.Settings, null, fixture.Logger));
        Assert.Throws<ArgumentNullException>(() => new MuleDispatcherHostedService(fixture.ScopeFactory, fixture.Queue, fixture.Executor, fixture.Signal, fixture.Settings, fixture.Metrics, null));
    }

    [Fact]
    public async Task Waits_Should_Handle_Missing_Due_Signal_And_Elapsed_Due()
    {
        var fixture = new DispatcherFixture(new MuleSettings());
        var dispatcher = fixture.Create();

        fixture.Signal.SignalRecovery();
        fixture.Signal.SignalCleanup();
        await InvokeAsync(dispatcher, "WaitForRecoveryAsync", null, CancellationToken.None);
        await InvokeAsync(dispatcher, "WaitForCleanupAsync", null, CancellationToken.None);

        fixture.Signal.SignalRecovery();
        fixture.Signal.SignalCleanup();
        await InvokeAsync(dispatcher, "WaitForRecoveryAsync", DateTimeOffset.UtcNow.AddSeconds(1), CancellationToken.None);
        await InvokeAsync(dispatcher, "WaitForCleanupAsync", DateTimeOffset.UtcNow.AddSeconds(1), CancellationToken.None);

        await InvokeStaticAsync(
            "WaitForSignalOrDueAsync",
            (Func<CancellationToken, Task>)(_ => Task.CompletedTask),
            DateTimeOffset.UtcNow.AddSeconds(1),
            CancellationToken.None);
        await InvokeStaticAsync(
            "WaitForSignalOrDueAsync",
            (Func<CancellationToken, Task>)(token => Task.Delay(TimeSpan.FromSeconds(5), token)),
            DateTimeOffset.UtcNow.AddMilliseconds(5),
            CancellationToken.None);
        await InvokeStaticAsync(
            "WaitForSignalOrDueAsync",
            (Func<CancellationToken, Task>)(_ => Task.CompletedTask),
            DateTimeOffset.UtcNow.AddMilliseconds(-1),
            CancellationToken.None);
    }

    [Fact]
    public async Task Dispatcher_Primitives_Should_Lock_Claim_Clean_And_Query_Storage()
    {
        var now = DateTimeOffset.UtcNow;
        var action = CreateAction(now);
        var storage = new ScriptedStorage
        {
            NextPending = now,
            OldestCompleted = now.AddDays(-2)
        };
        var fixture = new DispatcherFixture(new MuleSettings
        {
            CompletedRetention = TimeSpan.Zero,
            CleanupBatchSize = 0,
            LockTimeout = TimeSpan.Zero
        }, storage);
        var dispatcher = fixture.Create();

        await InvokeAsync(dispatcher, "DispatchQueuedActionAsync", new MuleDispatchItem(action.Id, action.Lane), CancellationToken.None);
        Assert.False(storage.Saved);

        storage.LockResult = action;
        await InvokeAsync(dispatcher, "DispatchQueuedActionAsync", new MuleDispatchItem(action.Id, action.Lane), CancellationToken.None);
        Assert.True(storage.Saved);

        storage.Saved = false;
        await InvokeAsync(dispatcher, "CleanCompletedAsync", CancellationToken.None);
        Assert.False(storage.Saved);
        storage.CleanResult = 2;
        await InvokeAsync(dispatcher, "CleanCompletedAsync", CancellationToken.None);
        Assert.True(storage.Saved);

        Assert.Equal(now, await InvokeAsync<DateTimeOffset?>(dispatcher, "GetNextPendingOnUtcAsync", CancellationToken.None));
        Assert.Equal(now.AddDays(-1), await InvokeAsync<DateTimeOffset?>(dispatcher, "GetNextCleanupOnUtcAsync", CancellationToken.None));
        storage.OldestCompleted = null;
        Assert.Null(await InvokeAsync<DateTimeOffset?>(dispatcher, "GetNextCleanupOnUtcAsync", CancellationToken.None));
    }

    [Fact]
    public async Task Claiming_Should_Cover_Empty_Single_Batch_Drain_Limits_And_Yield()
    {
        var now = DateTimeOffset.UtcNow;
        var storage = new ScriptedStorage();
        var settings = new MuleSettings
        {
            DrainUntilEmpty = true,
            MaxDrainBatchesPerCycle = 3,
            MaxDrainActionsPerCycle = 2,
            DispatchBatchSize = 1,
            YieldBetweenDrainBatches = TimeSpan.FromMilliseconds(1)
        };
        settings.Lanes["bounded"] = new MuleLaneSettings
        {
            DrainUntilEmpty = false,
            DispatchBatchSize = 2,
            MaxDrainBatchesPerCycle = 1,
            MaxDrainActionsPerCycle = 1
        };
        var fixture = new DispatcherFixture(settings, storage);
        var dispatcher = fixture.Create();

        storage.Claims.Enqueue([]);
        await InvokeAsync(dispatcher, "ClaimAndDispatchLaneAsync", MuleSettings.DefaultLane, CancellationToken.None);

        storage.Claims.Enqueue([CreateAction(now)]);
        await InvokeAsync(dispatcher, "ClaimAndDispatchLaneAsync", "bounded", CancellationToken.None);

        storage.Claims.Enqueue([CreateAction(now)]);
        storage.Claims.Enqueue([CreateAction(now.AddMilliseconds(1))]);
        await InvokeAsync(dispatcher, "ClaimAndDispatchLaneAsync", MuleSettings.DefaultLane, CancellationToken.None);

        Assert.Equal(4, storage.ClaimCalls);

        var yieldingStorage = new ScriptedStorage();
        yieldingStorage.Claims.Enqueue([CreateAction(now)]);
        yieldingStorage.Claims.Enqueue([]);
        var yieldingFixture = new DispatcherFixture(new MuleSettings
        {
            DrainUntilEmpty = true,
            MaxDrainBatchesPerCycle = 3,
            DispatchBatchSize = 1,
            YieldBetweenDrainBatches = TimeSpan.Zero
        }, yieldingStorage);
        await InvokeAsync(yieldingFixture.Create(), "ClaimAndDispatchLaneAsync", MuleSettings.DefaultLane, CancellationToken.None);

        var batchLimitedStorage = new ScriptedStorage();
        batchLimitedStorage.Claims.Enqueue([CreateAction(now)]);
        batchLimitedStorage.Claims.Enqueue([CreateAction(now.AddMilliseconds(1))]);
        var batchLimitedFixture = new DispatcherFixture(new MuleSettings
        {
            DrainUntilEmpty = true,
            MaxDrainBatchesPerCycle = 2,
            DispatchBatchSize = 1
        }, batchLimitedStorage);
        await InvokeAsync(batchLimitedFixture.Create(), "ClaimAndDispatchLaneAsync", MuleSettings.DefaultLane, CancellationToken.None);
        Assert.Equal(2, batchLimitedStorage.ClaimCalls);

        using var alreadyCanceled = new CancellationTokenSource();
        alreadyCanceled.Cancel();
        await InvokeAsync(
            new DispatcherFixture(new MuleSettings()).Create(),
            "ClaimAndDispatchLaneAsync",
            MuleSettings.DefaultLane,
            alreadyCanceled.Token);
    }

    [Fact]
    public async Task Queue_Worker_Loops_Should_Observe_Errors_Then_Cancellation()
    {
        using var cancellation = new CancellationTokenSource();
        var queue = new ScriptedQueue(cancellation);
        var fixture = new DispatcherFixture(new MuleSettings(), queue: queue);
        var dispatcher = fixture.Create();

        await InvokeAsync(dispatcher, "ProcessQueuedActionsAsync", MuleSettings.DefaultLane, cancellation.Token);

        using var unassignedCancellation = new CancellationTokenSource();
        queue.Reset(unassignedCancellation);
        await InvokeAsync(dispatcher, "ProcessUnassignedQueuedActionsAsync", Array.Empty<string>(), unassignedCancellation.Token);

        using var successCancellation = new CancellationTokenSource();
        var successQueue = new SuccessThenCancelQueue(successCancellation);
        var successDispatcher = new DispatcherFixture(new MuleSettings(), queue: successQueue).Create();
        await InvokeAsync(successDispatcher, "ProcessUnassignedQueuedActionsAsync", Array.Empty<string>(), successCancellation.Token);
    }

    [Fact]
    public async Task Recovery_And_Cleanup_Loops_Should_Log_Errors_And_Stop_On_Cancellation()
    {
        var settings = new MuleSettings
        {
            DispatchInterval = TimeSpan.FromMilliseconds(1),
            CleanupInterval = TimeSpan.FromMilliseconds(1)
        };

        using var pollingRecoveryCancellation = new CancellationTokenSource();
        var pollingRecoveryStorage = new ScriptedStorage();
        pollingRecoveryStorage.ClaimHandler = (call, token) => call == 1
            ? Task.FromException<IReadOnlyCollection<DurableAction>>(new InvalidOperationException("claim"))
            : CancelClaims(pollingRecoveryCancellation, token);
        await InvokeAsync(
            new DispatcherFixture(settings, pollingRecoveryStorage).Create(),
            "RecoverPendingActionsByPollingAsync",
            pollingRecoveryCancellation.Token);

        using var scheduledRecoveryCancellation = new CancellationTokenSource();
        var scheduledRecoveryStorage = new ScriptedStorage
        {
            NextPendingHandler = token => CancelNextPending(scheduledRecoveryCancellation, token)
        };
        scheduledRecoveryStorage.ClaimHandler = (call, _) => call == 1
            ? Task.FromException<IReadOnlyCollection<DurableAction>>(new InvalidOperationException("claim"))
            : Task.FromResult<IReadOnlyCollection<DurableAction>>([]);
        await InvokeAsync(
            new DispatcherFixture(settings, scheduledRecoveryStorage).Create(),
            "RecoverPendingActionsOnScheduleAsync",
            scheduledRecoveryCancellation.Token);

        using var pollingCleanupCancellation = new CancellationTokenSource();
        var pollingCleanupStorage = new ScriptedStorage();
        pollingCleanupStorage.CleanHandler = (call, token) => call == 1
            ? Task.FromException<int>(new InvalidOperationException("clean"))
            : CancelClean(pollingCleanupCancellation, token);
        await InvokeAsync(
            new DispatcherFixture(settings, pollingCleanupStorage).Create(),
            "CleanCompletedByPollingAsync",
            pollingCleanupCancellation.Token);

        using var scheduledCleanupCancellation = new CancellationTokenSource();
        var scheduledCleanupStorage = new ScriptedStorage
        {
            OldestCompletedHandler = token => CancelOldestCompleted(scheduledCleanupCancellation, token)
        };
        scheduledCleanupStorage.CleanHandler = (call, _) => call == 1
            ? Task.FromException<int>(new InvalidOperationException("clean"))
            : Task.FromResult(0);
        await InvokeAsync(
            new DispatcherFixture(settings, scheduledCleanupStorage).Create(),
            "CleanCompletedOnScheduleAsync",
            scheduledCleanupCancellation.Token);
    }

    [Fact]
    public async Task Configuration_Helpers_Should_Normalize_Global_And_Lane_Overrides()
    {
        var settings = new MuleSettings
        {
            WorkerCount = 0,
            DispatchBatchSize = 0,
            MaxDrainBatchesPerCycle = 0,
            MaxDrainActionsPerCycle = 0,
            DrainUntilEmpty = false,
            YieldBetweenDrainBatches = TimeSpan.FromMilliseconds(3),
            DispatchInterval = TimeSpan.Zero,
            CleanupInterval = TimeSpan.Zero,
            CompletedRetention = TimeSpan.Zero,
            CleanupBatchSize = 0,
            LockTimeout = TimeSpan.Zero
        };
        settings.Lanes["custom"] = new MuleLaneSettings
        {
            WorkerCount = 2,
            DispatchBatchSize = 3,
            MaxDrainBatchesPerCycle = 4,
            MaxDrainActionsPerCycle = 5,
            DrainUntilEmpty = true,
            YieldBetweenDrainBatches = TimeSpan.FromMilliseconds(2),
            PollingInterval = TimeSpan.FromMilliseconds(4),
            Priority = -1,
            Weight = 200
        };
        var dispatcher = new DispatcherFixture(settings).Create();

        Assert.Equal(1, Invoke<int>(dispatcher, "GetWorkerCount", "missing"));
        Assert.Equal(2, Invoke<int>(dispatcher, "GetWorkerCount", "custom"));
        Assert.Equal(1, Invoke<int>(dispatcher, "GetDispatchBatchSize", "missing"));
        Assert.Equal(3, Invoke<int>(dispatcher, "GetDispatchBatchSize", "custom"));
        Assert.Equal(0, Invoke<int>(dispatcher, "GetPriority", "missing"));
        Assert.Equal(-1, Invoke<int>(dispatcher, "GetPriority", "custom"));
        Assert.Equal(1, Invoke<int>(dispatcher, "GetWeight", "missing"));
        Assert.Equal(100, Invoke<int>(dispatcher, "GetWeight", "custom"));
        Assert.Equal(1, Invoke<int>(dispatcher, "GetMaxDrainBatchesPerCycle", "missing"));
        Assert.Equal(4, Invoke<int>(dispatcher, "GetMaxDrainBatchesPerCycle", "custom"));
        Assert.Null(Invoke<int?>(dispatcher, "GetRemainingDrainActions", "missing", 0));
        Assert.Equal(0, Invoke<int?>(dispatcher, "GetRemainingDrainActions", "custom", 6));
        Assert.False(Invoke<bool>(dispatcher, "GetDrainUntilEmpty", "missing"));
        Assert.True(Invoke<bool>(dispatcher, "GetDrainUntilEmpty", "custom"));
        Assert.Equal(TimeSpan.FromMilliseconds(3), Invoke<TimeSpan>(dispatcher, "GetYieldBetweenDrainBatches", "missing"));
        Assert.Equal(TimeSpan.FromMilliseconds(2), Invoke<TimeSpan>(dispatcher, "GetYieldBetweenDrainBatches", "custom"));
        Assert.Equal(TimeSpan.FromMinutes(1), Invoke<TimeSpan>(dispatcher, "GetPollingInterval", "missing"));
        Assert.Equal(TimeSpan.FromMilliseconds(4), Invoke<TimeSpan>(dispatcher, "GetPollingInterval", "custom"));
        Assert.Equal(TimeSpan.FromMinutes(1), Invoke<TimeSpan>(dispatcher, "GetDispatchInterval"));
        Assert.Equal(TimeSpan.FromMinutes(10), Invoke<TimeSpan>(dispatcher, "GetCleanupInterval"));
        Assert.Equal(TimeSpan.FromDays(1), Invoke<TimeSpan>(dispatcher, "GetCompletedRetention"));
        Assert.Equal(1, Invoke<int>(dispatcher, "GetCleanupBatchSize"));
        Assert.Equal(TimeSpan.FromMinutes(5), Invoke<TimeSpan>(dispatcher, "GetLockTimeout"));
        Assert.Equal(MuleSettings.DefaultLane, Invoke<string>(dispatcher, "NormalizeLane", " "));
        Assert.Contains("custom", Invoke<IReadOnlyCollection<string>>(dispatcher, "GetFairLaneNames"));

        var positive = new DispatcherFixture(new MuleSettings
        {
            DispatchInterval = TimeSpan.FromSeconds(2),
            CleanupInterval = TimeSpan.FromSeconds(3),
            CompletedRetention = TimeSpan.FromHours(4),
            LockTimeout = TimeSpan.FromSeconds(5)
        }).Create();
        Assert.Equal(TimeSpan.FromSeconds(2), Invoke<TimeSpan>(positive, "GetDispatchInterval"));
        Assert.Equal(TimeSpan.FromSeconds(3), Invoke<TimeSpan>(positive, "GetCleanupInterval"));
        Assert.Equal(TimeSpan.FromHours(4), Invoke<TimeSpan>(positive, "GetCompletedRetention"));
        Assert.Equal(TimeSpan.FromSeconds(5), Invoke<TimeSpan>(positive, "GetLockTimeout"));

        var dueNow = DateTimeOffset.UtcNow;
        Assert.True(Invoke<bool>(positive, "IsLaneDueForPolling", "lane", dueNow));
        var polling = Invoke<Dictionary<string, DateTimeOffset>>(positive, "GetPollingSchedule");
        polling["lane"] = dueNow.AddMinutes(1);
        Assert.False(Invoke<bool>(positive, "IsLaneDueForPolling", "lane", dueNow));
        polling["lane"] = dueNow.AddMinutes(-1);
        Assert.True(Invoke<bool>(positive, "IsLaneDueForPolling", "lane", dueNow));
        polling[MuleSettings.DefaultLane] = dueNow.AddMinutes(-1);
        await InvokeAsync(positive, "DelayUntilNextLanePollingAsync", dueNow, CancellationToken.None);

        var withoutSchedule = new DispatcherFixture(new MuleSettings()).Create();
        await InvokeAsync(withoutSchedule, "DelayUntilNextLanePollingAsync", dueNow, CancellationToken.None);
    }

    [Fact]
    public void Executor_Helpers_Should_Normalize_Custom_And_Legacy_Retry_Settings()
    {
        var globalPolicySettings = new MuleSettings
        {
            RetryPolicy = new MuleRetryPolicy { MaxAttempts = 0, Delay = TimeSpan.Zero },
            CompletedRetention = TimeSpan.Zero
        };
        var globalExecutor = new DispatcherFixture(globalPolicySettings).Executor;
        var globalPolicy = Invoke<MuleRetryPolicy>(globalExecutor, "GetRetryPolicy", "missing");
        Assert.Equal(1, globalPolicy.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(30), globalPolicy.Delay);
        Assert.Equal(TimeSpan.FromDays(1), Invoke<TimeSpan>(globalExecutor, "GetCompletedRetention"));

        var positivePolicySettings = new MuleSettings
        {
            RetryPolicy = new MuleRetryPolicy { MaxAttempts = 4, Delay = TimeSpan.FromSeconds(2) }
        };
        var positivePolicy = Invoke<MuleRetryPolicy>(
            new DispatcherFixture(positivePolicySettings).Executor,
            "GetRetryPolicy",
            "missing");
        Assert.Equal(4, positivePolicy.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(2), positivePolicy.Delay);

        var legacySettings = new MuleSettings
        {
            MaxAttempts = 0,
            RetryDelay = TimeSpan.Zero,
            CompletedRetention = TimeSpan.FromHours(2)
        };
        legacySettings.Lanes["custom"] = new MuleLaneSettings
        {
            MaxAttempts = 3,
            RetryDelay = TimeSpan.FromSeconds(2)
        };
        legacySettings.Lanes["fallback"] = new MuleLaneSettings();
        var legacyExecutor = new DispatcherFixture(legacySettings).Executor;
        var custom = Invoke<MuleRetryPolicy>(legacyExecutor, "GetRetryPolicy", "custom");
        var fallback = Invoke<MuleRetryPolicy>(legacyExecutor, "GetRetryPolicy", "fallback");
        var missing = Invoke<MuleRetryPolicy>(legacyExecutor, "GetRetryPolicy", "missing");

        Assert.Equal(3, custom.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(2), custom.Delay);
        Assert.Equal(1, fallback.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(30), fallback.Delay);
        Assert.Equal(1, missing.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(30), missing.Delay);
        Assert.Equal(TimeSpan.FromHours(2), Invoke<TimeSpan>(legacyExecutor, "GetCompletedRetention"));
        Assert.Equal(MuleSettings.DefaultLane, Invoke<string>(legacyExecutor, "NormalizeLane", ""));
        Assert.Equal("custom", Invoke<string>(legacyExecutor, "NormalizeLane", "custom"));
    }

    private static Task<IReadOnlyCollection<DurableAction>> CancelClaims(CancellationTokenSource source, CancellationToken token)
    {
        source.Cancel();
        return Task.FromException<IReadOnlyCollection<DurableAction>>(new OperationCanceledException(token));
    }

    private static Task<DateTimeOffset?> CancelNextPending(CancellationTokenSource source, CancellationToken token)
    {
        source.Cancel();
        return Task.FromException<DateTimeOffset?>(new OperationCanceledException(token));
    }

    private static Task<int> CancelClean(CancellationTokenSource source, CancellationToken token)
    {
        source.Cancel();
        return Task.FromException<int>(new OperationCanceledException(token));
    }

    private static Task<DateTimeOffset?> CancelOldestCompleted(CancellationTokenSource source, CancellationToken token)
    {
        source.Cancel();
        return Task.FromException<DateTimeOffset?>(new OperationCanceledException(token));
    }

    private static DurableAction CreateAction(DateTimeOffset now)
        => new()
        {
            Key = Key,
            Lane = MuleSettings.DefaultLane,
            Payload = "{}",
            PayloadType = typeof(object).AssemblyQualifiedName,
            CreatedOnUtc = now
        };

    private static T Invoke<T>(object target, string method, params object[] arguments)
    {
        if (method == "GetPollingSchedule")
        {
            var field = target.GetType().GetField("_nextLanePollingOnUtc", BindingFlags.Instance | BindingFlags.NonPublic);
            return (T)field.GetValue(target);
        }

        return (T)GetMethod(target.GetType(), method).Invoke(target, arguments);
    }

    private static async Task<T> InvokeAsync<T>(object target, string method, params object[] arguments)
        => await (Task<T>)GetMethod(target.GetType(), method).Invoke(target, arguments);

    private static async Task InvokeAsync(object target, string method, params object[] arguments)
        => await (Task)GetMethod(target.GetType(), method).Invoke(target, arguments);

    private static async Task InvokeStaticAsync(string method, params object[] arguments)
        => await (Task)GetMethod(typeof(MuleDispatcherHostedService), method).Invoke(null, arguments);

    private static MethodInfo GetMethod(Type type, string name)
        => type.GetMethod(name, BindingFlags.Instance | BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Method '{name}' was not found.");

    private sealed class DispatcherFixture
    {
        private readonly ServiceProvider _services;

        public DispatcherFixture(MuleSettings settings, IMuleStorage storage = null, IMuleDispatchQueue queue = null)
        {
            storage ??= new ScriptedStorage();
            _services = new ServiceCollection()
                .AddSingleton<IMuleStorage>(storage)
                .AddSingleton(new MuleActionRegistry())
                .BuildServiceProvider();
            Settings = Options.Create(settings);
            Queue = queue ?? new ChannelMuleDispatchQueue(Settings);
            Signal = new MuleSchedulerSignal();
            Metrics = new MuleRuntimeMetrics();
            ScopeFactory = _services.GetRequiredService<IServiceScopeFactory>();
            Logger = NullLogger<MuleDispatcherHostedService>.Instance;
            Executor = new MuleActionExecutor(ScopeFactory, Signal, Settings, Metrics, NullLogger<MuleActionExecutor>.Instance);
            var stopped = new CancellationTokenSource();
            stopped.Cancel();
            Executor.Start(stopped.Token);
        }

        public IServiceScopeFactory ScopeFactory { get; }
        public IMuleDispatchQueue Queue { get; }
        public MuleActionExecutor Executor { get; }
        public MuleSchedulerSignal Signal { get; }
        public IOptions<MuleSettings> Settings { get; }
        public MuleRuntimeMetrics Metrics { get; }
        public ILogger<MuleDispatcherHostedService> Logger { get; }

        public MuleDispatcherHostedService Create()
            => new(ScopeFactory, Queue, Executor, Signal, Settings, Metrics, Logger);
    }

    private sealed class ScriptedQueue : IMuleDispatchQueue
    {
        private CancellationTokenSource _cancellation;
        private int _laneCalls;
        private int _unassignedCalls;

        public ScriptedQueue(CancellationTokenSource cancellation) => _cancellation = cancellation;

        public void Reset(CancellationTokenSource cancellation)
        {
            _cancellation = cancellation;
            _unassignedCalls = 0;
        }

        public ValueTask EnqueueAsync(Ulid actionId, string lane, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public ValueTask<MuleDispatchItem> DequeueAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public ValueTask<MuleDispatchItem> DequeueAsync(string lane, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _laneCalls) == 1)
                throw new InvalidOperationException("queue failure");

            _cancellation.Cancel();
            throw new OperationCanceledException(_cancellation.Token);
        }

        public ValueTask<MuleDispatchItem> DequeueUnassignedAsync(IReadOnlyCollection<string> assignedLanes, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _unassignedCalls) == 1)
                throw new InvalidOperationException("queue failure");

            _cancellation.Cancel();
            throw new OperationCanceledException(_cancellation.Token);
        }
    }

    private sealed class SuccessThenCancelQueue : IMuleDispatchQueue
    {
        private readonly CancellationTokenSource _cancellation;
        private int _calls;

        public SuccessThenCancelQueue(CancellationTokenSource cancellation) => _cancellation = cancellation;
        public ValueTask EnqueueAsync(Ulid actionId, string lane, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public ValueTask<MuleDispatchItem> DequeueAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask<MuleDispatchItem> DequeueAsync(string lane, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask<MuleDispatchItem> DequeueUnassignedAsync(IReadOnlyCollection<string> assignedLanes, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _calls) == 1)
                return ValueTask.FromResult(new MuleDispatchItem(Ulid.NewUlid(), MuleSettings.DefaultLane));

            _cancellation.Cancel();
            throw new OperationCanceledException(_cancellation.Token);
        }
    }

    private sealed class ScriptedStorage : IMuleStorage
    {
        public Queue<IReadOnlyCollection<DurableAction>> Claims { get; } = new();
        public DurableAction LockResult { get; set; }
        public DateTimeOffset? NextPending { get; set; }
        public DateTimeOffset? OldestCompleted { get; set; }
        public int CleanResult { get; set; }
        public int ClaimCalls { get; private set; }
        public bool Saved { get; set; }
        public Func<int, CancellationToken, Task<IReadOnlyCollection<DurableAction>>> ClaimHandler { get; set; }
        public Func<int, CancellationToken, Task<int>> CleanHandler { get; set; }
        public Func<CancellationToken, Task<DateTimeOffset?>> NextPendingHandler { get; set; }
        public Func<CancellationToken, Task<DateTimeOffset?>> OldestCompletedHandler { get; set; }
        private int _cleanCalls;

        public Task AddAsync(DurableAction action, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task AddRangeAsync(IReadOnlyCollection<DurableAction> actions, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<Ulid?> FindByDeduplicationKeyAsync(ActionKey key, string deduplicationKey, CancellationToken cancellationToken = default) => Task.FromResult<Ulid?>(null);
        public Task<IReadOnlyCollection<DurableAction>> ClaimPendingAsync(string lane, int batchSize, TimeSpan lockTimeout, DateTimeOffset now, CancellationToken cancellationToken = default)
        {
            ClaimCalls++;
            if (ClaimHandler != null)
                return ClaimHandler(ClaimCalls, cancellationToken);
            return Task.FromResult(Claims.Count == 0 ? (IReadOnlyCollection<DurableAction>)[] : Claims.Dequeue());
        }
        public Task<DateTimeOffset?> GetNextPendingOnUtcAsync(TimeSpan lockTimeout, DateTimeOffset now, CancellationToken cancellationToken = default)
            => NextPendingHandler?.Invoke(cancellationToken) ?? Task.FromResult(NextPending);
        public Task<DurableAction> LockAsync(Ulid id, TimeSpan lockTimeout, DateTimeOffset now, CancellationToken cancellationToken = default) => Task.FromResult(LockResult);
        public Task MarkCompletedAsync(Ulid id, DateTimeOffset completedOnUtc, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task MarkFailedAsync(Ulid id, string error, DateTimeOffset now, DateTimeOffset? nextAttemptOnUtc, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<int> CleanCompletedAsync(DateTimeOffset olderThanUtc, int batchSize, CancellationToken cancellationToken = default)
            => CleanHandler?.Invoke(Interlocked.Increment(ref _cleanCalls), cancellationToken) ?? Task.FromResult(CleanResult);
        public Task<DateTimeOffset?> GetOldestCompletedOnUtcAsync(CancellationToken cancellationToken = default)
            => OldestCompletedHandler?.Invoke(cancellationToken) ?? Task.FromResult(OldestCompleted);
        public Task SaveChangesAsync(CancellationToken cancellationToken = default) { Saved = true; return Task.CompletedTask; }
    }
}
