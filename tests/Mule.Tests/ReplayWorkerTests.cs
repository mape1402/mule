namespace Mule.Tests;

using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Mule.Dispatching;
using Mule.Diagnostics;
using Mule.Replay;
using NSubstitute;

public sealed class ReplayWorkerTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.replay.worker.v1");

    [Fact]
    public async Task Polling_Worker_Should_Process_Empty_Cycles_Until_Stopped()
    {
        var storage = new WorkerStorage();
        using var provider = CreateProvider(storage);
        var settings = new MuleSettings();
        settings.Replay.RecoveryMode = MuleRecoveryMode.Polling;
        settings.Replay.PollingInterval = TimeSpan.FromMilliseconds(5);
        var worker = CreateWorker(provider, settings);

        await worker.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => storage.ClaimCalls >= 2);
        await worker.StopAsync(CancellationToken.None);

        Assert.True(storage.ClaimCalls >= 2);
    }

    [Fact]
    public async Task Scheduled_Worker_Should_Wake_For_Signal_And_Due_Time()
    {
        var storage = new WorkerStorage
        {
            NextDueOnUtc = DateTimeOffset.UtcNow.AddMilliseconds(25)
        };
        using var provider = CreateProvider(storage);
        var signal = new MuleSchedulerSignal();
        var worker = CreateWorker(provider, new MuleSettings(), signal);

        await worker.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => storage.NextCalls >= 1);
        signal.SignalReplayAt(DateTimeOffset.UtcNow);
        await WaitUntilAsync(() => storage.NextCalls >= 3);
        await worker.StopAsync(CancellationToken.None);

        Assert.True(storage.NextCalls >= 3);
    }

    [Fact]
    public async Task Worker_Should_Ignore_Lost_Lease_And_Notify_Materialized_Action()
    {
        var claim = CreateClaim();
        var storage = new WorkerStorage();
        storage.Claims.Enqueue([claim]);
        storage.Claims.Enqueue([claim]);
        storage.Outcomes.Enqueue(MuleReplayMaterializationOutcome.LeaseLost);
        storage.Outcomes.Enqueue(MuleReplayMaterializationOutcome.Reconciled);
        using var provider = CreateProvider(storage);
        var notifier = Substitute.For<IMuleCommitNotifier>();
        var settings = new MuleSettings();
        settings.Replay.BatchSize = 1;
        var worker = CreateWorker(provider, settings, commitNotifier: notifier);

        await worker.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => storage.MaterializeCalls >= 2);
        await worker.StopAsync(CancellationToken.None);

        await notifier.Received(1).NotifySavedAsync(
            claim.Replay.ReplayActionId,
            claim.SourceAction.Lane,
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Worker_Should_Release_A_Failed_Materialization()
    {
        var storage = new WorkerStorage { MaterializationError = new InvalidOperationException("planned") };
        storage.Claims.Enqueue([CreateClaim()]);
        using var provider = CreateProvider(storage);
        var settings = new MuleSettings();
        settings.Replay.RetryDelay = TimeSpan.FromMilliseconds(5);
        var worker = CreateWorker(provider, settings);

        await worker.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => storage.ReleaseCalls == 1);
        await worker.StopAsync(CancellationToken.None);

        Assert.Contains("planned", storage.LastReleasedError);
    }

    [Fact]
    public async Task Worker_Should_Keep_Running_When_Claim_Or_Release_Fails()
    {
        var storage = new WorkerStorage
        {
            ClaimErrorCount = 1,
            MaterializationError = new InvalidOperationException("materialize"),
            ReleaseError = new InvalidOperationException("release")
        };
        storage.Claims.Enqueue([CreateClaim()]);
        using var provider = CreateProvider(storage);
        var settings = new MuleSettings();
        settings.Replay.RecoveryMode = MuleRecoveryMode.Polling;
        settings.Replay.PollingInterval = TimeSpan.FromMilliseconds(5);
        settings.Replay.RetryDelay = TimeSpan.FromMilliseconds(1);
        var worker = CreateWorker(provider, settings);

        await worker.StartAsync(CancellationToken.None);
        await WaitUntilAsync(() => storage.ReleaseCalls == 1);
        await worker.StopAsync(CancellationToken.None);

        Assert.True(storage.ClaimCalls >= 2);
    }

    [Fact]
    public void Worker_Should_Validate_Dependencies()
    {
        using var provider = CreateProvider(new WorkerStorage());
        var scopes = provider.GetRequiredService<IServiceScopeFactory>();
        var notifier = Substitute.For<IMuleCommitNotifier>();
        var signal = new MuleSchedulerSignal();
        var options = Options.Create(new MuleSettings());
        var metrics = new MuleRuntimeMetrics();
        var logger = NullLogger<MuleReplayHostedService>.Instance;

        Assert.Throws<ArgumentNullException>(() => new MuleReplayHostedService(null, notifier, signal, options, TimeProvider.System, metrics, logger));
        Assert.Throws<ArgumentNullException>(() => new MuleReplayHostedService(scopes, null, signal, options, TimeProvider.System, metrics, logger));
        Assert.Throws<ArgumentNullException>(() => new MuleReplayHostedService(scopes, notifier, null, options, TimeProvider.System, metrics, logger));
        Assert.Throws<ArgumentNullException>(() => new MuleReplayHostedService(scopes, notifier, signal, null, TimeProvider.System, metrics, logger));
        Assert.Throws<ArgumentNullException>(() => new MuleReplayHostedService(scopes, notifier, signal, new NullOptions<MuleSettings>(), TimeProvider.System, metrics, logger));
        Assert.Throws<ArgumentNullException>(() => new MuleReplayHostedService(scopes, notifier, signal, options, null, metrics, logger));
        Assert.Throws<ArgumentNullException>(() => new MuleReplayHostedService(scopes, notifier, signal, options, TimeProvider.System, null, logger));
        Assert.Throws<ArgumentNullException>(() => new MuleReplayHostedService(scopes, notifier, signal, options, TimeProvider.System, metrics, null));
    }

    [Fact]
    public async Task Worker_Should_Normalize_NonPositive_Intervals_And_Exit_A_PreCancelled_Poll()
    {
        using var provider = CreateProvider(new WorkerStorage());
        var settings = new MuleSettings();
        settings.Replay.LockTimeout = TimeSpan.Zero;
        settings.Replay.RetryDelay = TimeSpan.Zero;
        settings.Replay.PollingInterval = TimeSpan.Zero;
        var worker = CreateWorker(provider, settings);

        Assert.Equal(TimeSpan.FromMinutes(1), InvokePrivate<TimeSpan>(worker, "GetLockTimeout"));
        Assert.Equal(TimeSpan.FromSeconds(10), InvokePrivate<TimeSpan>(worker, "GetRetryDelay"));
        Assert.Equal(TimeSpan.FromMinutes(1), InvokePrivate<TimeSpan>(worker, "GetPollingInterval"));

        var polling = InvokePrivate<Task>(worker, "RunByPollingAsync", new CancellationToken(true));
        await polling;
    }

    [Fact]
    public async Task Worker_Should_Wake_When_The_Next_Replay_Becomes_Due()
    {
        using var provider = CreateProvider(new WorkerStorage());
        var worker = CreateWorker(provider, new MuleSettings());

        await InvokePrivate<Task>(
            worker,
            "WaitForSignalOrDueAsync",
            (DateTimeOffset?)DateTimeOffset.UtcNow.AddMilliseconds(50),
            CancellationToken.None);
    }

    private static ServiceProvider CreateProvider(IMuleReplayStorage storage)
        => new ServiceCollection()
            .AddSingleton(storage)
            .AddSingleton<IMuleReplayStorage>(storage)
            .BuildServiceProvider();

    private static MuleReplayHostedService CreateWorker(
        ServiceProvider provider,
        MuleSettings settings,
        MuleSchedulerSignal signal = null,
        IMuleCommitNotifier commitNotifier = null)
        => new(
            provider.GetRequiredService<IServiceScopeFactory>(),
            commitNotifier ?? Substitute.For<IMuleCommitNotifier>(),
            signal ?? new MuleSchedulerSignal(),
            Options.Create(settings),
            TimeProvider.System,
            new MuleRuntimeMetrics(),
            NullLogger<MuleReplayHostedService>.Instance);

    private static MuleReplayClaim CreateClaim()
    {
        var sourceId = Ulid.NewUlid();
        return new MuleReplayClaim
        {
            Replay = new MuleReplay
            {
                RequestId = Ulid.NewUlid(),
                SourceActionId = sourceId,
                ReplayRootActionId = sourceId,
                ReplayActionId = Ulid.NewUlid(),
                ActionKey = Key,
                Lane = MuleSettings.DefaultLane
            },
            SourceAction = new DurableAction
            {
                Id = sourceId,
                Key = Key,
                Lane = MuleSettings.DefaultLane,
                Payload = "{}",
                PayloadType = typeof(object).AssemblyQualifiedName,
                Status = DurableActionStatus.Failed
            },
            LockToken = Ulid.NewUlid()
        };
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        while (!condition())
            await Task.Delay(5, timeout.Token);
    }

    private static T InvokePrivate<T>(object target, string name, params object[] arguments)
        => (T)target.GetType()
            .GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic)
            .Invoke(target, arguments);

    private sealed class NullOptions<T> : IOptions<T>
        where T : class
    {
        public T Value => null;
    }

    private sealed class WorkerStorage : IMuleReplayStorage
    {
        public Queue<IReadOnlyCollection<MuleReplayClaim>> Claims { get; } = new();

        public Queue<MuleReplayMaterializationOutcome> Outcomes { get; } = new();

        public int ClaimCalls { get; private set; }

        public int NextCalls { get; private set; }

        public int MaterializeCalls { get; private set; }

        public int ReleaseCalls { get; private set; }

        public int ClaimErrorCount { get; set; }

        public DateTimeOffset? NextDueOnUtc { get; set; }

        public Exception MaterializationError { get; set; }

        public Exception ReleaseError { get; set; }

        public string LastReleasedError { get; private set; }

        public Task<MuleReplayResult> ScheduleAsync(Ulid sourceActionId, MuleReplayRequest request, DateTimeOffset now, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<MuleReplayInfo> FindAsync(Ulid requestId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<IReadOnlyCollection<MuleReplayClaim>> ClaimScheduledAsync(
            int batchSize,
            TimeSpan lockTimeout,
            DateTimeOffset now,
            CancellationToken cancellationToken = default)
        {
            ClaimCalls++;
            if (ClaimErrorCount-- > 0)
                throw new InvalidOperationException("claim");

            return Task.FromResult(Claims.TryDequeue(out var claims)
                ? claims
                : (IReadOnlyCollection<MuleReplayClaim>)Array.Empty<MuleReplayClaim>());
        }

        public Task<DateTimeOffset?> GetNextScheduledOnUtcAsync(
            TimeSpan lockTimeout,
            DateTimeOffset now,
            CancellationToken cancellationToken = default)
        {
            NextCalls++;
            return Task.FromResult(NextDueOnUtc);
        }

        public Task<MuleReplayMaterializationOutcome> MaterializeAsync(
            MuleReplayClaim claim,
            DurableAction action,
            DateTimeOffset now,
            CancellationToken cancellationToken = default)
        {
            MaterializeCalls++;
            if (MaterializationError != null)
                throw MaterializationError;

            return Task.FromResult(Outcomes.TryDequeue(out var outcome)
                ? outcome
                : MuleReplayMaterializationOutcome.Created);
        }

        public Task ReleaseAsync(
            MuleReplayClaim claim,
            string error,
            DateTimeOffset nextAttemptOnUtc,
            CancellationToken cancellationToken = default)
        {
            ReleaseCalls++;
            LastReleasedError = error;
            if (ReleaseError != null)
                throw ReleaseError;

            return Task.CompletedTask;
        }
    }
}
