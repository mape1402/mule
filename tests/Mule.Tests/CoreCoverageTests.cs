namespace Mule.Tests;

using System.Text.Json;
using System.Transactions;
using System.Reflection;
using System.Reflection.Emit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Mule.Configuration;
using Mule.Diagnostics;
using Mule.Dispatching;
using NSubstitute;

public sealed class CoreCoverageTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.core.action");

    [Fact]
    public void ValueObjects_Should_Cover_Equality_And_Defaults()
    {
        var empty = default(ActionKey);
        Assert.Equal(string.Empty, empty.GetHashCode().ToString().Substring(0, 0));
        Assert.True(ActionKey.From("a.b").Equals((object)ActionKey.From("a.b")));
        Assert.True(ActionKey.From("a.b") == ActionKey.From("a.b"));
        Assert.True(ActionKey.From("a.b") != ActionKey.From("a.c"));
        Assert.False(ActionKey.From("a.b").Equals("a.b"));

        Assert.Equal("00000000000000000000000000", Ulid.Empty.ToString());
        var first = Ulid.Parse("01ARZ3NDEKTSV4RRFFQ69G5FAV");
        var second = Ulid.Parse("01ARZ3NDEKTSV4RRFFQ69G5FAW");
        Assert.True(first.CompareTo((object)second) < 0);
        Assert.True(first != second);
        var same = first;
        Assert.False(first != same);
        Assert.True(first < second);
        Assert.True(second > first);
        Assert.True(first <= same);
        Assert.True(second >= first);
        Assert.True(first.Equals((object)same));
        Assert.False(first.Equals("not-an-ulid"));
        Assert.True(Ulid.TryParse("O1ARZ3NDEKTSV4RRFFQ69G5FAV", out var normalized));
        Assert.Equal(first, normalized);
        Assert.Equal(1, Ulid.NewUlid().CompareTo(null));
        Assert.Throws<ArgumentException>(() => Ulid.NewUlid().CompareTo("bad"));
        Assert.Throws<FormatException>(() => Ulid.Parse("bad"));
    }

    [Fact]
    public void RetryPolicy_Should_Normalize_Delay_MaxAttempts_And_Jitter()
    {
        var policy = new MuleRetryPolicy
        {
            Delay = TimeSpan.Zero,
            Backoff = MuleRetryBackoff.Exponential,
            JitterRatio = 2
        };

        var delay = policy.GetDelay(0);

        Assert.InRange(delay, TimeSpan.Zero, TimeSpan.FromMinutes(1));
    }

    [Fact]
    public void ActionContext_Should_Expose_Action_Fields_And_Metadata()
    {
        var action = new DurableAction
        {
            Key = Key,
            Lane = "critical",
            CorrelationId = "corr",
            DeduplicationKey = "dedupe",
            Metadata = JsonSerializer.Serialize(new Dictionary<string, string> { ["tenant"] = "elysium" })
        };
        using var services = new ServiceCollection().BuildServiceProvider();

        var context = new MuleActionContext(action, services);

        Assert.Equal(action.Id, context.Id);
        Assert.Equal(Key, context.Key);
        Assert.Equal("critical", context.Lane);
        Assert.Equal("corr", context.CorrelationId);
        Assert.Equal("dedupe", context.DeduplicationKey);
        Assert.Equal("elysium", context.Metadata["tenant"]);
        Assert.Same(services, context.Services);
        Assert.Empty(new MuleActionContext(new DurableAction(), services).Metadata);
        Assert.Empty(new MuleActionContext(new DurableAction { Metadata = "null" }, services).Metadata);
        Assert.Throws<ArgumentNullException>(() => new MuleActionContext(null, services));
        Assert.Throws<ArgumentNullException>(() => new MuleActionContext(action, null));
    }

    [Fact]
    public async Task MuleClient_Should_Create_Save_Deduplicate_And_Notify_Actions()
    {
        var storage = new RecordingStorage();
        var notifier = new RecordingNotifier();
        var client = new MuleClient(storage, new JsonMuleSerializer(), notifier);

        var ids = await client.EnqueueManyAsync(
        [
            MuleIntent.For(Key, new Payload("one"), options =>
            {
                options.Lane = " ";
                options.CorrelationId = "corr";
                options.DeduplicationKey = "same";
                options.Metadata["source"] = "test";
            }),
            MuleIntent.For(Key, new Payload("two"))
        ]);

        Assert.Equal(2, ids.Count);
        Assert.True(storage.Saved);
        Assert.Equal(2, storage.Actions.Count);
        Assert.Equal(MuleSettings.DefaultLane, storage.Actions[0].Lane);
        Assert.Contains("\"source\":\"test\"", storage.Actions[0].Metadata);
        Assert.Equal(storage.DeduplicatedId, ids.First());
        Assert.Equal(2, notifier.Notifications.Count);

        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.EnqueueManyAsync(null));
        await Assert.ThrowsAsync<ArgumentException>(async () => await client.EnqueueManyAsync([null]));
        await Assert.ThrowsAsync<ArgumentException>(async () => await client.EnqueueManyAsync([MuleIntent.For(default(ActionKey), new Payload("invalid"))]));
        await Assert.ThrowsAsync<ArgumentException>(async () => await client.EnqueueManyAsync([MuleIntent.For(Key, (Payload)null)]));
        Assert.Empty(await client.EnqueueManyAsync(Array.Empty<MuleIntent>()));

        storage.ReturnDeduplicatedId = false;
        var nonDuplicateId = await client.EnqueueAsync(Key, new Payload("not-found"), options => options.DeduplicationKey = "absent");
        Assert.Equal(storage.Actions.Last().Id, nonDuplicateId);

        Assert.Throws<ArgumentNullException>(() => new MuleClient(null, new JsonMuleSerializer(), notifier));
        Assert.Throws<ArgumentNullException>(() => new MuleClient(storage, null, notifier));
        Assert.Throws<ArgumentNullException>(() => new MuleClient(storage, new JsonMuleSerializer(), null));
    }

    [Fact]
    public async Task ActionHandlers_Should_Resolve_Services_Deserialize_And_Validate_Payloads()
    {
        var services = new ServiceCollection()
            .AddSingleton<IMuleSerializer, JsonMuleSerializer>()
            .AddSingleton<ServiceActionProbe>()
            .BuildServiceProvider();
        var action = CreateAction(new Payload("hello"));
        var serviceHandler = new MuleActionHandler<ServiceActionProbe, Payload>(
            Key,
            static (service, context, token) =>
            {
                service.Values.Add(context.Payload.Value);
                return ValueTask.CompletedTask;
            });
        var activatorHandler = new ActivatorMuleActionHandler<ActivatorAction, Payload>(Key);
        var reflectedHandler = new ActivatorMuleActionHandler(Key, typeof(ActivatorAction), typeof(Payload));

        await serviceHandler.ExecuteAsync(action, services, CancellationToken.None);
        await activatorHandler.ExecuteAsync(action, services, CancellationToken.None);
        await reflectedHandler.ExecuteAsync(action, services, CancellationToken.None);

        var probe = services.GetRequiredService<ServiceActionProbe>();
        Assert.Equal(["hello", "hello", "hello"], probe.Values);
        Assert.Throws<ArgumentException>(() => new MuleActionHandler<ServiceActionProbe, Payload>(default, (_, _, _) => ValueTask.CompletedTask));
        Assert.Throws<ArgumentNullException>(() => new MuleActionHandler<ServiceActionProbe, Payload>(Key, null));
        Assert.Throws<ArgumentException>(() => new ActivatorMuleActionHandler<ActivatorAction, Payload>(default));
        Assert.Throws<ArgumentException>(() => new ActivatorMuleActionHandler(default, typeof(ActivatorAction), typeof(Payload)));
        Assert.Throws<ArgumentNullException>(() => new ActivatorMuleActionHandler(Key, null, typeof(Payload)));
        Assert.Throws<ArgumentNullException>(() => new ActivatorMuleActionHandler(Key, typeof(ActivatorAction), null));

        var wrongPayload = CreateAction("wrong");
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await reflectedHandler.ExecuteAsync(wrongPayload, services, CancellationToken.None));
        var missingMethodHandler = new ActivatorMuleActionHandler(
            ActionKey.From("tests.core.missing-method"),
            typeof(ServiceActionProbe),
            typeof(Payload));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await missingMethodHandler.ExecuteAsync(action, services, CancellationToken.None));
    }

    [Fact]
    public void Registry_Should_Add_Find_And_Reject_Duplicates()
    {
        var registry = new MuleActionRegistry();
        var handler = new MuleActionHandler<ServiceActionProbe, Payload>(
            Key,
            (_, _, _) => ValueTask.CompletedTask);

        registry.Add(handler);

        Assert.True(registry.TryGet(Key, out var found));
        Assert.Same(handler, found);
        Assert.True(registry.TryGetHandler(Key, out var typed));
        Assert.Same(handler, typed);
        Assert.False(registry.TryGet(ActionKey.From("missing"), out _));
        Assert.Throws<ArgumentNullException>(() => registry.Add(null));
        Assert.Throws<InvalidOperationException>(() => registry.Add(handler));
    }

    [Fact]
    public void RegistrationBuilder_Should_Configure_Services_Actions_And_Runtime()
    {
        var services = new ServiceCollection();
        services.AddOptions<MuleSettings>();
        var registry = new MuleActionRegistry();
        var builder = new MuleRegistrationBuilder(services, registry);

        builder
            .Configure(settings => settings.MaxAttempts = 3)
            .AddServices(collection => collection.AddSingleton<ServiceActionProbe>())
            .UseRecovery(MuleRecoveryMode.Scheduled)
            .UseCleanup(MuleCleanupMode.Disabled)
            .For<ActivatorAction, Payload>(Key)
            .For<ServiceActionProbe, Payload>(ActionKey.From("tests.core.service"), (_, _, _) => ValueTask.CompletedTask)
            .For<ServiceActionProbe, Payload>(ActionKey.From("tests.core.service.task"), (_, _, _) => Task.CompletedTask)
            .ConfigureHighThroughputRuntime()
            .ConfigureLane("critical", lane =>
            {
                lane.Priority = 10;
                lane.Weight = 2;
            });

        builder.AddActionsFromAssemblyContaining<DiscoveredAction>();

        using var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredService<IOptions<MuleSettings>>().Value;
        Assert.NotNull(provider.GetRequiredService<ServiceActionProbe>());
        Assert.Equal(MuleRecoveryMode.Scheduled, settings.RecoveryMode);
        Assert.Equal(MuleCleanupMode.Disabled, settings.CleanupMode);
        Assert.Equal(10, settings.Lanes["critical"].Priority);
        Assert.True(registry.TryGetHandler(Key, out _));
        Assert.True(registry.TryGetHandler(ActionKey.From("tests.core.discovered"), out _));
        Assert.Throws<ArgumentNullException>(() => new MuleRegistrationBuilder(null, registry));
        Assert.Throws<ArgumentNullException>(() => new MuleRegistrationBuilder(services, null));
        Assert.Throws<ArgumentNullException>(() => builder.Configure(null));
        Assert.Throws<ArgumentNullException>(() => builder.AddServices(null));
        Assert.Throws<ArgumentNullException>(() => builder.AddActionsFromAssembly(null));
        Assert.Throws<ArgumentNullException>(() => builder.For<ServiceActionProbe, Payload>(ActionKey.From("tests.core.null-task"), (Func<ServiceActionProbe, MuleActionContext<Payload>, CancellationToken, Task>)null));
        Assert.Throws<ArgumentNullException>(() => MuleHighThroughputRegistrationBuilderExtensions.ConfigureHighThroughputRuntime(null));
        Assert.Throws<ArgumentNullException>(() => MuleHighThroughputRegistrationBuilderExtensions.ConfigureLane(null, "x", _ => { }));
        Assert.Throws<ArgumentException>(() => builder.ConfigureLane(" ", _ => { }));
        Assert.Throws<ArgumentNullException>(() => builder.ConfigureLane("x", null));
        Assert.Throws<ArgumentNullException>(() => MuleServiceCollectionExtensions.AddMule(null));

        builder.ConfigureHighThroughputRuntime().ConfigureLane("critical", lane => lane.Weight = 3);
        using var tunedProvider = services.BuildServiceProvider();
        var tuned = tunedProvider.GetRequiredService<IOptions<MuleSettings>>().Value;
        Assert.Equal(3, tuned.Lanes["critical"].Weight);
        Assert.True(tuned.YieldBetweenDrainBatches > TimeSpan.Zero);
        Assert.True(tuned.DispatchInterval <= TimeSpan.FromSeconds(5));

        var lane = new MuleLaneSettings
        {
            WorkerCount = 1,
            MaxDegreeOfParallelism = 2,
            DispatchBatchSize = 3,
            MaxDrainBatchesPerCycle = 4,
            MaxDrainActionsPerCycle = 5,
            DrainUntilEmpty = true,
            YieldBetweenDrainBatches = TimeSpan.FromMilliseconds(1),
            DispatchQueueCapacity = 6,
            ExecutionQueueCapacity = 7,
            PollingInterval = TimeSpan.FromSeconds(1),
            MaxAttempts = 8,
            RetryDelay = TimeSpan.FromSeconds(2),
            RetryPolicy = new MuleRetryPolicy(),
            Priority = 9,
            Weight = 10
        };
        Assert.Equal(1, lane.WorkerCount);
        Assert.Equal(2, lane.MaxDegreeOfParallelism);
        Assert.Equal(3, lane.DispatchBatchSize);
        Assert.Equal(4, lane.MaxDrainBatchesPerCycle);
        Assert.Equal(5, lane.MaxDrainActionsPerCycle);
        Assert.True(lane.DrainUntilEmpty);
        Assert.Equal(TimeSpan.FromMilliseconds(1), lane.YieldBetweenDrainBatches);
        Assert.Equal(6, lane.DispatchQueueCapacity);
        Assert.Equal(7, lane.ExecutionQueueCapacity);
        Assert.Equal(TimeSpan.FromSeconds(1), lane.PollingInterval);
        Assert.Equal(8, lane.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(2), lane.RetryDelay);
        Assert.NotNull(lane.RetryPolicy);
        Assert.Equal(9, lane.Priority);
        Assert.Equal(10, lane.Weight);
    }

    [Fact]
    public void RegistrationBuilder_Should_Reject_Attributed_Actions_With_Multiple_Contracts()
    {
        var assembly = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName($"Mule.InvalidActions.{Guid.NewGuid():N}"),
            AssemblyBuilderAccess.Run);
        var module = assembly.DefineDynamicModule("main");
        var type = module.DefineType(
            "InvalidAction",
            TypeAttributes.Class | TypeAttributes.Abstract | TypeAttributes.Public);
        type.AddInterfaceImplementation(typeof(IDisposable));
        type.AddInterfaceImplementation(typeof(IMuleAction<string>));
        type.AddInterfaceImplementation(typeof(IMuleAction<int>));
        var attributeConstructor = typeof(MuleActionAttribute).GetConstructor([typeof(string)]);
        type.SetCustomAttribute(new CustomAttributeBuilder(attributeConstructor, ["tests.invalid.dynamic"]));
        var invalidType = type.CreateType();
        var builder = new MuleRegistrationBuilder(new ServiceCollection(), new MuleActionRegistry());
        var add = typeof(MuleRegistrationBuilder).GetMethod("AddDiscoveredAction", BindingFlags.Instance | BindingFlags.NonPublic);

        var exception = Assert.Throws<TargetInvocationException>(() => add.Invoke(builder, [invalidType]));

        Assert.IsType<InvalidOperationException>(exception.InnerException);
    }

    [Fact]
    public async Task SchedulerSignal_Should_Wait_For_Immediate_And_Delayed_Signals()
    {
        var signal = new MuleSchedulerSignal();

        signal.SignalRecovery();
        signal.SignalCleanup();

        await signal.WaitForRecoveryAsync(CancellationToken.None);
        await signal.WaitForCleanupAsync(CancellationToken.None);

        signal.SignalRecoveryAt(DateTimeOffset.UtcNow.AddMilliseconds(-1));
        signal.SignalCleanupAt(DateTimeOffset.UtcNow.AddMilliseconds(1));

        await signal.WaitForRecoveryAsync(CancellationToken.None);
        await signal.WaitForCleanupAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ChannelQueue_Should_Dequeue_By_Lane_And_Unassigned_Lanes()
    {
        var settings = Options.Create(new MuleSettings
        {
            DispatchQueueCapacity = 1
        });
        settings.Value.Lanes["critical"] = new MuleLaneSettings
        {
            Priority = 10,
            Weight = 2,
            DispatchQueueCapacity = 2
        };
        var queue = new ChannelMuleDispatchQueue(settings);
        var defaultId = Ulid.NewUlid();
        var criticalId = Ulid.NewUlid();

        await queue.EnqueueAsync(defaultId, null);
        await queue.EnqueueAsync(criticalId, "critical");

        var critical = await queue.DequeueAsync("critical");
        var unassigned = await queue.DequeueUnassignedAsync(["critical"]);

        Assert.Equal(criticalId, critical.ActionId);
        Assert.Equal("critical", critical.Lane);
        Assert.Equal(defaultId, unassigned.ActionId);
        Assert.Equal(MuleSettings.DefaultLane, unassigned.Lane);

        Assert.Throws<ArgumentNullException>(() => new ChannelMuleDispatchQueue(null));

        var unbounded = new ChannelMuleDispatchQueue(Options.Create(new MuleSettings()));
        var id = Ulid.NewUlid();
        await unbounded.EnqueueAsync(id, "other");
        Assert.Equal(id, (await unbounded.DequeueAsync()).ActionId);

        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => unbounded.DequeueAsync("empty", canceled.Token).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => unbounded.DequeueAsync(canceled.Token).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => unbounded.DequeueUnassignedAsync([], canceled.Token).AsTask());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => unbounded.DequeueUnassignedAsync([MuleSettings.DefaultLane, "empty"], canceled.Token).AsTask());

        var emptyQueue = new ChannelMuleDispatchQueue(Options.Create(new MuleSettings()));
        var signals = typeof(ChannelMuleDispatchQueue).GetField("_signals", BindingFlags.Instance | BindingFlags.NonPublic)
            .GetValue(emptyQueue);
        var writer = signals.GetType().GetProperty("Writer").GetValue(signals);
        writer.GetType().GetMethod("TryWrite").Invoke(writer, [true]);
        using var activeCancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(20));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            emptyQueue.DequeueUnassignedAsync([MuleSettings.DefaultLane], activeCancellation.Token).AsTask());
    }

    [Fact]
    public async Task CommitNotifier_Should_Respect_ImmediateDispatch_And_Transactions()
    {
        var queue = Substitute.For<IMuleDispatchQueue>();
        var signal = new MuleSchedulerSignal();
        var scheduled = new AmbientTransactionMuleCommitNotifier(
            queue,
            signal,
            Options.Create(new MuleSettings
            {
                ImmediateDispatch = false,
                RecoveryMode = MuleRecoveryMode.Scheduled
            }));

        await scheduled.NotifySavedAsync(Ulid.NewUlid(), "lane");
        await signal.WaitForRecoveryAsync(CancellationToken.None);
        await queue.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default);

        var immediate = new AmbientTransactionMuleCommitNotifier(
            queue,
            signal,
            Options.Create(new MuleSettings()));
        var id = Ulid.NewUlid();

        await immediate.NotifySavedAsync(id, "lane");

        await queue.Received(1).EnqueueAsync(id, "lane", Arg.Any<CancellationToken>());

        var transactionalQueue = Substitute.For<IMuleDispatchQueue>();
        var transactional = new AmbientTransactionMuleCommitNotifier(
            transactionalQueue,
            signal,
            Options.Create(new MuleSettings()));
        var committedId = Ulid.NewUlid();
        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await transactional.NotifySavedAsync(committedId, "tx");
            await transactionalQueue.DidNotReceiveWithAnyArgs().EnqueueAsync(default, default);
            scope.Complete();
        }

        await WaitUntilAsync(async () =>
        {
            try
            {
                await transactionalQueue.Received(1).EnqueueAsync(committedId, "tx", Arg.Any<CancellationToken>());
                return true;
            }
            catch
            {
                return false;
            }
        });

        var rolledBackId = Ulid.NewUlid();
        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await transactional.NotifySavedAsync(rolledBackId, "tx");
        }

        await Task.Delay(25);

        Assert.Throws<ArgumentNullException>(() => new AmbientTransactionMuleCommitNotifier(null, signal, Options.Create(new MuleSettings())));
        Assert.Throws<ArgumentNullException>(() => new AmbientTransactionMuleCommitNotifier(queue, null, Options.Create(new MuleSettings())));
        Assert.Throws<ArgumentNullException>(() => new AmbientTransactionMuleCommitNotifier(queue, signal, null));
        await transactionalQueue.DidNotReceive().EnqueueAsync(rolledBackId, "tx", Arg.Any<CancellationToken>());

        var throwingQueue = Substitute.For<IMuleDispatchQueue>();
        throwingQueue.EnqueueAsync(Arg.Any<Ulid>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<ValueTask>(_ => throw new InvalidOperationException("queue failed"));
        var throwing = new AmbientTransactionMuleCommitNotifier(
            throwingQueue,
            signal,
            Options.Create(new MuleSettings()));
        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await throwing.NotifySavedAsync(Ulid.NewUlid(), "tx");
            scope.Complete();
        }

        await Task.Delay(25);
    }

    [Fact]
    public void Metrics_Should_Record_And_Trim_Runtime_State()
    {
        var metrics = new MuleRuntimeMetrics();
        var key = ActionKey.From("metrics.test");
        var now = DateTimeOffset.UtcNow;

        metrics.RecordCompleted(null, key, now.AddMinutes(-2));
        metrics.RecordCompleted("blue", key, now);
        metrics.RecordFailed("blue", key);
        metrics.RecordDuplicateIgnored();
        metrics.RecordClaimed("blue", 0);
        metrics.RecordClaimed("blue", 3);
        metrics.RecordExecutorSaturation("blue");
        metrics.RecordWaitingExecution("blue", now.AddSeconds(-5));
        metrics.RecordExecutionStarted("blue", now.AddSeconds(-5), now);
        metrics.RecordExecutionStarted("blue", now.AddSeconds(-4), now);
        metrics.RecordExecutionStarted("never-queued", now.AddSeconds(-3), now);
        metrics.RecordExecutionStopped("blue");
        metrics.RecordExecutionStopped("blue");
        metrics.RecordExecutionStopped("never-queued");
        metrics.RecordWaitingExecutionCanceled("missing");

        var snapshot = metrics.GetSnapshot(now);

        Assert.Equal(2, snapshot.Completed);
        Assert.Equal(1, snapshot.ThroughputPerMinute);
        Assert.Equal(1, snapshot.Failed);
        Assert.Equal(1, snapshot.DuplicatesIgnored);
        Assert.Equal(3, snapshot.ClaimedByLane["blue"]);
        Assert.Equal(1, snapshot.ExecutorSaturationByLane["blue"]);
        Assert.False(snapshot.WaitingExecutionByLane.ContainsKey("blue"));
        Assert.False(snapshot.ExecutingByLane.ContainsKey("blue"));
    }

    [Fact]
    public async Task ActionExecutor_Should_Validate_Inputs_Apply_Custom_Retry_And_Record_Cancellations()
    {
        var storage = new RecordingStorage();
        var services = new ServiceCollection()
            .AddSingleton<IMuleStorage>(storage)
            .AddSingleton(new MuleActionRegistry())
            .BuildServiceProvider();
        var settings = Options.Create(new MuleSettings
        {
            MaxDegreeOfParallelism = 1,
            ExecutionQueueCapacity = 1,
            RetryPolicy = new MuleRetryPolicy
            {
                MaxAttempts = 2,
                Delay = TimeSpan.Zero,
                Backoff = MuleRetryBackoff.Linear,
                JitterRatio = 0.5
            }
        });
        var executor = new MuleActionExecutor(
            services.GetRequiredService<IServiceScopeFactory>(),
            new MuleSchedulerSignal(),
            settings,
            new MuleRuntimeMetrics(),
            NullLogger<MuleActionExecutor>.Instance);

        await Assert.ThrowsAsync<ArgumentNullException>(() => executor.EnqueueAsync(null).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => executor.EnqueueRangeAsync(null).AsTask());
        await Assert.ThrowsAsync<ArgumentException>(() => executor.EnqueueRangeAsync([null]).AsTask());

        using var running = new CancellationTokenSource();
        executor.Start(running.Token);
        await executor.EnqueueAsync(CreateStoredAction("missing-handler"));
        await storage.Failed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await running.CancelAsync();

        Assert.NotNull(storage.NextAttemptOnUtc);

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        var saturated = new MuleActionExecutor(
            services.GetRequiredService<IServiceScopeFactory>(),
            new MuleSchedulerSignal(),
            Options.Create(new MuleSettings
            {
                ExecutionQueueCapacity = 1,
                MaxDegreeOfParallelism = 1
            }),
            new MuleRuntimeMetrics(),
            NullLogger<MuleActionExecutor>.Instance);
        saturated.Start(canceled.Token);
        await saturated.EnqueueAsync(CreateStoredAction("queued"));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            saturated.EnqueueAsync(CreateStoredAction("blocked"), canceled.Token).AsTask());

        Assert.Throws<ArgumentNullException>(() => new MuleActionExecutor(null, new MuleSchedulerSignal(), settings, new MuleRuntimeMetrics(), NullLogger<MuleActionExecutor>.Instance));
        Assert.Throws<ArgumentNullException>(() => new MuleActionExecutor(services.GetRequiredService<IServiceScopeFactory>(), null, settings, new MuleRuntimeMetrics(), NullLogger<MuleActionExecutor>.Instance));
        Assert.Throws<ArgumentNullException>(() => new MuleActionExecutor(services.GetRequiredService<IServiceScopeFactory>(), new MuleSchedulerSignal(), null, new MuleRuntimeMetrics(), NullLogger<MuleActionExecutor>.Instance));
        Assert.Throws<ArgumentNullException>(() => new MuleActionExecutor(services.GetRequiredService<IServiceScopeFactory>(), new MuleSchedulerSignal(), settings, null, NullLogger<MuleActionExecutor>.Instance));
        Assert.Throws<ArgumentNullException>(() => new MuleActionExecutor(services.GetRequiredService<IServiceScopeFactory>(), new MuleSchedulerSignal(), settings, new MuleRuntimeMetrics(), null));
    }

    [Fact]
    public async Task ActionExecutor_Should_Complete_Registered_Actions_And_Signal_Scheduled_Cleanup()
    {
        var storage = new RecordingStorage();
        var registry = new MuleActionRegistry();
        var probe = new ServiceActionProbe();
        registry.Add(new MuleActionHandler<ServiceActionProbe, Payload>(
            Key,
            (service, context, _) =>
            {
                service.Values.Add(context.Payload.Value);
                return ValueTask.CompletedTask;
            }));
        using var services = new ServiceCollection()
            .AddSingleton<IMuleStorage>(storage)
            .AddSingleton(registry)
            .AddSingleton(probe)
            .AddSingleton<IMuleSerializer, JsonMuleSerializer>()
            .BuildServiceProvider();
        var signal = new MuleSchedulerSignal();
        var settings = Options.Create(new MuleSettings
        {
            RecoveryMode = MuleRecoveryMode.Scheduled,
            CleanupMode = MuleCleanupMode.Scheduled,
            CompletedRetention = TimeSpan.FromMilliseconds(1),
            ExecutionQueueCapacity = 0,
            MaxDegreeOfParallelism = 0
        });
        settings.Value.Lanes["custom"] = new MuleLaneSettings
        {
            MaxDegreeOfParallelism = 2,
            ExecutionQueueCapacity = 2,
            RetryPolicy = new MuleRetryPolicy
            {
                MaxAttempts = 1,
                Delay = TimeSpan.FromMilliseconds(1),
                MaxDelay = TimeSpan.FromSeconds(1),
                Backoff = MuleRetryBackoff.Fixed
            }
        };
        var executor = new MuleActionExecutor(
            services.GetRequiredService<IServiceScopeFactory>(),
            signal,
            settings,
            new MuleRuntimeMetrics(),
            NullLogger<MuleActionExecutor>.Instance);

        using var running = new CancellationTokenSource();
        executor.Start(running.Token);
        await executor.WaitForCapacityAsync(MuleSettings.DefaultLane);
        var action = CreateStoredAction("completed");
        action.Lane = "custom";
        await executor.EnqueueAsync(action);
        await storage.Completed.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await signal.WaitForCleanupAsync(CancellationToken.None);
        await running.CancelAsync();

        Assert.Contains("completed", probe.Values);
    }

    [Fact]
    public async Task ActionExecutor_Worker_Should_Contain_Storage_Save_Failures()
    {
        var storage = new RecordingStorage { ThrowOnSave = true };
        using var services = new ServiceCollection()
            .AddSingleton<IMuleStorage>(storage)
            .AddSingleton(new MuleActionRegistry())
            .BuildServiceProvider();
        var executor = new MuleActionExecutor(
            services.GetRequiredService<IServiceScopeFactory>(),
            new MuleSchedulerSignal(),
            Options.Create(new MuleSettings()),
            new MuleRuntimeMetrics(),
            NullLogger<MuleActionExecutor>.Instance);

        using var running = new CancellationTokenSource();
        executor.Start(running.Token);
        await executor.EnqueueAsync(CreateStoredAction("save-failure"));
        await storage.SaveAttempted.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await running.CancelAsync();
    }

    private static DurableAction CreateAction<T>(T payload)
        => new()
        {
            Key = Key,
            Payload = JsonSerializer.Serialize(payload, new JsonSerializerOptions(JsonSerializerDefaults.Web)),
            PayloadType = payload.GetType().AssemblyQualifiedName
        };

    private static DurableAction CreateStoredAction(string value)
        => CreateAction(new Payload(value));

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(2);
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await condition())
                return;

            await Task.Delay(10);
        }

        Assert.Fail("Condition was not met before timeout.");
    }

    private sealed record Payload(string Value);

    private sealed class ServiceActionProbe
    {
        public List<string> Values { get; } = new();
    }

    private sealed class ActivatorAction : IMuleAction<Payload>
    {
        private readonly ServiceActionProbe _probe;

        public ActivatorAction(ServiceActionProbe probe)
        {
            _probe = probe;
        }

        public ValueTask ExecuteAsync(MuleActionContext<Payload> context, CancellationToken cancellationToken)
        {
            _probe.Values.Add(context.Payload.Value);
            return ValueTask.CompletedTask;
        }
    }

    [MuleAction("tests.core.discovered")]
    private sealed class DiscoveredAction : IMuleAction<Payload>
    {
        public ValueTask ExecuteAsync(MuleActionContext<Payload> context, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;
    }

    private sealed class InvalidDiscoveredAction : IMuleAction<Payload>, IMuleAction<string>
    {
        public ValueTask ExecuteAsync(MuleActionContext<Payload> context, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;

        public ValueTask ExecuteAsync(MuleActionContext<string> context, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;
    }

    private sealed class RecordingNotifier : IMuleCommitNotifier
    {
        public List<(Ulid Id, string Lane)> Notifications { get; } = new();

        public ValueTask NotifySavedAsync(Ulid actionId, string lane, CancellationToken cancellationToken = default)
        {
            Notifications.Add((actionId, lane));
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RecordingStorage : IMuleStorage
    {
        public Ulid DeduplicatedId { get; } = Ulid.NewUlid();
        public List<DurableAction> Actions { get; } = new();
        public bool Saved { get; private set; }
        public DateTimeOffset? NextAttemptOnUtc { get; private set; }
        public TaskCompletionSource Failed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource SaveAttempted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public bool ReturnDeduplicatedId { get; set; } = true;
        public bool ThrowOnSave { get; set; }

        public Task AddAsync(DurableAction action, CancellationToken cancellationToken = default)
        {
            Actions.Add(action);
            return Task.CompletedTask;
        }

        public Task AddRangeAsync(IReadOnlyCollection<DurableAction> actions, CancellationToken cancellationToken = default)
        {
            Actions.AddRange(actions);
            return Task.CompletedTask;
        }

        public Task<Ulid?> FindByDeduplicationKeyAsync(ActionKey key, string deduplicationKey, CancellationToken cancellationToken = default)
            => Task.FromResult(ReturnDeduplicatedId ? (Ulid?)DeduplicatedId : null);

        public Task<IReadOnlyCollection<DurableAction>> ClaimPendingAsync(string lane, int batchSize, TimeSpan lockTimeout, DateTimeOffset now, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyCollection<DurableAction>>(Array.Empty<DurableAction>());

        public Task<DateTimeOffset?> GetNextPendingOnUtcAsync(TimeSpan lockTimeout, DateTimeOffset now, CancellationToken cancellationToken = default)
            => Task.FromResult<DateTimeOffset?>(null);

        public Task<DurableAction> LockAsync(Ulid id, TimeSpan lockTimeout, DateTimeOffset now, CancellationToken cancellationToken = default)
            => Task.FromResult<DurableAction>(null);

        public Task MarkCompletedAsync(Ulid id, DateTimeOffset completedOnUtc, CancellationToken cancellationToken = default)
        {
            Completed.TrySetResult();
            return Task.CompletedTask;
        }

        public Task MarkFailedAsync(Ulid id, string error, DateTimeOffset now, DateTimeOffset? nextAttemptOnUtc, CancellationToken cancellationToken = default)
        {
            NextAttemptOnUtc = nextAttemptOnUtc;
            Failed.TrySetResult();
            return Task.CompletedTask;
        }

        public Task<int> CleanCompletedAsync(DateTimeOffset olderThanUtc, int batchSize, CancellationToken cancellationToken = default)
            => Task.FromResult(0);

        public Task<DateTimeOffset?> GetOldestCompletedOnUtcAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<DateTimeOffset?>(null);

        public Task SaveChangesAsync(CancellationToken cancellationToken = default)
        {
            SaveAttempted.TrySetResult();
            if (ThrowOnSave)
                throw new InvalidOperationException("save failed");

            Saved = true;
            return Task.CompletedTask;
        }
    }
}
