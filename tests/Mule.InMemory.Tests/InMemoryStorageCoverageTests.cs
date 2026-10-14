namespace Mule.InMemory.Tests;

using System.Reflection;
using System.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Mule.Diagnostics;

public sealed class InMemoryStorageCoverageTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.inmemory.coverage");

    [Fact]
    public void Store_Should_Handle_Deduplication_Locking_Terminals_Cleanup_And_Snapshot()
    {
        var store = new InMemoryMuleStore();
        var now = DateTimeOffset.UtcNow;
        var due = CreateAction("due", now.AddSeconds(-3));
        var future = CreateAction("future", now.AddSeconds(-2), nextAttemptOnUtc: now.AddMinutes(5));
        var lockedExpired = CreateAction("expired", now.AddSeconds(-1), DurableActionStatus.Locked, lockedOnUtc: now.AddMinutes(-10));
        var lockedActive = CreateAction("active", now, DurableActionStatus.Locked, lockedOnUtc: now);
        var completedOld = CreateAction("old", now.AddMinutes(-20), DurableActionStatus.Completed, completedOnUtc: now.AddMinutes(-10));
        var failed = CreateAction("failed", now.AddMinutes(-1), DurableActionStatus.Failed);
        failed.Attempts = 2;

        Assert.True(store.Add(due));
        Assert.False(store.Add(due));
        Assert.True(store.Add(future));
        Assert.True(store.Add(lockedExpired));
        Assert.True(store.Add(lockedActive));
        Assert.True(store.Add(completedOld));
        Assert.True(store.Add(failed));
        Assert.False(store.Add(CreateAction("dedupe-duplicate", now, deduplicationKey: due.DeduplicationKey)));

        Assert.Null(store.FindByDeduplicationKey(Key, ""));
        Assert.Equal(due.Id, store.FindByDeduplicationKey(Key, due.DeduplicationKey));
        Assert.Null(store.Lock(Ulid.NewUlid(), TimeSpan.FromMinutes(1), now));
        Assert.Null(store.Lock(lockedActive.Id, TimeSpan.FromMinutes(5), now));

        var claimed = store.ClaimPending("Lane-A", 10, TimeSpan.FromMinutes(1), now).ToArray();

        Assert.Contains(claimed, x => x.Id == due.Id);
        Assert.Contains(claimed, x => x.Id == lockedExpired.Id);
        Assert.DoesNotContain(claimed, x => x.Id == future.Id);
        Assert.All(claimed, x => Assert.Equal(DurableActionStatus.Locked, x.Status));
        Assert.NotNull(store.GetNextPendingOnUtc(TimeSpan.FromMinutes(1), now));

        var retry = claimed.Single(x => x.Id == due.Id);
        store.MarkFailed(retry.Id, "try again", now.AddSeconds(5));
        var terminal = claimed.Single(x => x.Id == lockedExpired.Id);
        store.MarkFailed(terminal.Id, "done", null);
        var lockedAgain = store.Lock(retry.Id, TimeSpan.FromMinutes(1), now.AddSeconds(6));
        store.MarkCompleted(lockedAgain.Id, now.AddSeconds(7));

        Assert.Throws<InvalidOperationException>(() => store.MarkCompleted(Ulid.NewUlid(), now));
        Assert.Equal(completedOld.CompletedOnUtc, store.GetOldestCompletedOnUtc());
        Assert.Equal(1, store.CleanCompleted(now.AddMinutes(-1), 1));

        var runtime = new MuleRuntimeMetrics();
        runtime.RecordCompleted("Lane-A", Key, now);
        runtime.RecordFailed("Lane-A", Key);
        runtime.RecordClaimed("Lane-A", 2);
        runtime.RecordWaitingExecution("Lane-A", now.AddSeconds(-1));
        var snapshot = store.GetSnapshot(TimeSpan.FromMinutes(1), runtime.GetSnapshot(now));

        Assert.Equal(1, snapshot.Pending);
        Assert.True(snapshot.Locked >= 1);
        Assert.True(snapshot.Completed >= 1);
        Assert.Equal(2, snapshot.Failed);
        Assert.True(snapshot.LockExpired >= 0);
        Assert.Equal(1, snapshot.RuntimeCompleted);
        Assert.Equal(1, snapshot.RuntimeFailed);
        Assert.True(snapshot.BacklogByLane.ContainsKey("Lane-A"));
        Assert.True(snapshot.FailuresByActionKey.ContainsKey(Key));
        Assert.True(snapshot.RetriesByActionKey.ContainsKey(Key));
        Assert.True(snapshot.CompletedPerMinuteByLane.ContainsKey("Lane-A"));
        Assert.NotNull(snapshot.AverageEnqueueToTerminalLatency);
    }

    [Fact]
    public void Store_Should_Cover_Future_Active_Lock_Default_Lane_And_Empty_Diagnostics()
    {
        var store = new InMemoryMuleStore();
        var now = DateTimeOffset.UtcNow;
        var future = CreateAction("future-only", now, nextAttemptOnUtc: now.AddMinutes(2));
        future.DeduplicationKey = null;
        future.Lane = null;
        Assert.True(store.Add(future));
        Assert.Empty(store.ClaimPending("other", 1, TimeSpan.FromMinutes(1), now));
        Assert.Equal(now.AddMinutes(2), store.GetNextPendingOnUtc(TimeSpan.FromMinutes(1), now));
        Assert.Null(store.Lock(future.Id, TimeSpan.FromMinutes(1), now));

        var due = CreateAction("default", now.AddSeconds(-1));
        due.Lane = " ";
        Assert.True(store.Add(due));
        var locked = store.Lock(due.Id, TimeSpan.FromMinutes(1), now);
        Assert.NotNull(locked);
        Assert.Equal(MuleSettings.DefaultLane, locked.Lane);
        Assert.Equal(now.AddMinutes(1), store.GetNextPendingOnUtc(TimeSpan.FromMinutes(1), now));
        Assert.Null(store.FindByDeduplicationKey(ActionKey.From("tests.inmemory.other"), "default"));

        var recentCompleted = CreateAction("recent-completed", now, DurableActionStatus.Completed, completedOnUtc: now.AddMinutes(1));
        Assert.True(store.Add(recentCompleted));
        Assert.Equal(0, store.CleanCompleted(now, 10));

        var empty = new InMemoryMuleStore().GetSnapshot(
            TimeSpan.FromMinutes(1),
            new MuleRuntimeMetrics().GetSnapshot(now));
        Assert.Null(empty.OldestPendingAge);
        Assert.Null(empty.OldestLockedAge);
        Assert.Null(empty.AverageEnqueueToExecutionLatency);
        Assert.Null(empty.AverageExecutionLatency);
        Assert.Null(empty.AverageEnqueueToTerminalLatency);

        var normalize = typeof(InMemoryMuleStore).GetMethod("NormalizeLane", BindingFlags.Static | BindingFlags.NonPublic);
        Assert.Equal(MuleSettings.DefaultLane, normalize.Invoke(null, [null]));
        Assert.Equal("lane", normalize.Invoke(null, ["lane"]));
        var backlog = typeof(InMemoryMuleStore).GetMethod("IsBacklog", BindingFlags.Static | BindingFlags.NonPublic);
        Assert.True((bool)backlog.Invoke(null, [future]));
        Assert.False((bool)backlog.Invoke(null, [recentCompleted]));
    }

    [Fact]
    public void Store_Should_Cover_All_Deduplication_And_Lock_Predicates()
    {
        var now = DateTimeOffset.UtcNow;
        var store = new InMemoryMuleStore();
        var baseAction = CreateAction("base", now.AddMinutes(-3), deduplicationKey: "shared");
        Assert.True(store.Add(baseAction));

        var otherKey = CreateAction("other-key", now, deduplicationKey: "shared");
        otherKey.Key = ActionKey.From("tests.inmemory.other");
        Assert.True(store.Add(otherKey));
        Assert.True(store.Add(CreateAction("other-dedupe", now, deduplicationKey: "different")));
        Assert.False(store.Add(CreateAction("exact", now, deduplicationKey: "shared")));

        var pendingDue = CreateAction("pending-due", now, nextAttemptOnUtc: now.AddSeconds(-1));
        var pendingFuture = CreateAction("pending-future", now, nextAttemptOnUtc: now.AddSeconds(1));
        var lockedExpired = CreateAction("locked-expired", now, DurableActionStatus.Locked, now.AddMinutes(-2));
        var lockedActive = CreateAction("locked-active", now, DurableActionStatus.Locked, now);
        var lockedWithoutDate = CreateAction("locked-null", now, DurableActionStatus.Locked);
        var completedWithoutDate = CreateAction("completed-null", now, DurableActionStatus.Completed);
        foreach (var action in new[] { pendingDue, pendingFuture, lockedExpired, lockedActive, lockedWithoutDate, completedWithoutDate })
            Assert.True(store.Add(action));

        Assert.NotNull(store.GetNextPendingOnUtc(TimeSpan.FromMinutes(1), now));
        Assert.NotEmpty(store.ClaimPending("Lane-A", 1, TimeSpan.FromMinutes(1), now));
        Assert.Empty(store.ClaimPending("missing", 10, TimeSpan.FromMinutes(1), now));
        var beforeLocks = store.GetSnapshot(TimeSpan.FromMinutes(1), new MuleRuntimeMetrics().GetSnapshot(now));
        Assert.True(beforeLocks.LockExpired > 0);

        Assert.NotNull(store.Lock(pendingDue.Id, TimeSpan.FromMinutes(1), now));
        Assert.Null(store.Lock(pendingFuture.Id, TimeSpan.FromMinutes(1), now));
        Assert.NotNull(store.Lock(lockedExpired.Id, TimeSpan.FromMinutes(1), now));
        Assert.Null(store.Lock(lockedActive.Id, TimeSpan.FromMinutes(1), now));
        Assert.Null(store.Lock(lockedWithoutDate.Id, TimeSpan.FromMinutes(1), now));
        Assert.Null(store.Lock(completedWithoutDate.Id, TimeSpan.FromMinutes(1), now));
        Assert.Equal(0, store.CleanCompleted(now, 10));

        var snapshot = store.GetSnapshot(TimeSpan.FromMinutes(1), new MuleRuntimeMetrics().GetSnapshot(now));
        Assert.True(snapshot.Locked > 0);
    }

    [Fact]
    public async Task Storage_Should_Defer_Commits_To_Ambient_Transactions()
    {
        var store = new InMemoryMuleStore();
        var metrics = new MuleRuntimeMetrics();
        var storage = new InMemoryMuleStorage(store, metrics);
        var committed = CreateAction("committed", DateTimeOffset.UtcNow);
        var rolledBack = CreateAction("rolled-back", DateTimeOffset.UtcNow);

        await storage.SaveChangesAsync();
        await storage.AddAsync(committed);

        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await storage.SaveChangesAsync();
            Assert.Empty(store.Actions);
            scope.Complete();
        }

        Assert.Single(store.Actions);

        await storage.AddAsync(rolledBack);
        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await storage.SaveChangesAsync();
        }

        Assert.Single(store.Actions);
        await Assert.ThrowsAsync<ArgumentNullException>(() => storage.AddAsync(null));
        await Assert.ThrowsAsync<ArgumentNullException>(() => storage.AddRangeAsync(null));
        await Assert.ThrowsAsync<ArgumentException>(() => storage.AddRangeAsync([null]));

        Assert.Throws<ArgumentNullException>(() => new InMemoryMuleStorage(null, metrics));
        Assert.Throws<ArgumentNullException>(() => new InMemoryMuleStorage(store, null));

        var duplicateStorage = new InMemoryMuleStorage(store, metrics);
        await duplicateStorage.AddAsync(committed);
        await duplicateStorage.SaveChangesAsync();
    }

    [Fact]
    public async Task Diagnostics_And_Registration_Should_Validate_Nulls_And_Register_Services()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentNullException>(() => InMemoryMuleServiceCollectionExtensions.UseInMemory(null));
        Assert.Throws<ArgumentNullException>(() => InMemoryMuleServiceCollectionExtensions.UseInMemoryMule(null));
        services.AddOptions<MuleSettings>();
        services.AddSingleton<MuleRuntimeMetrics>();
        services.UseInMemoryMule();
        using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetRequiredService<IInMemoryMule>());
        Assert.NotNull(provider.GetRequiredService<IMuleDiagnostics>());
        Assert.Throws<ArgumentNullException>(() => new InMemoryMule(null));
        Assert.Throws<ArgumentNullException>(() => new InMemoryMuleDiagnostics(null, Options.Create(new MuleSettings()), new MuleRuntimeMetrics()));
        Assert.Throws<ArgumentNullException>(() => new InMemoryMuleDiagnostics(new InMemoryMuleStore(), null, new MuleRuntimeMetrics()));
        Assert.Throws<ArgumentNullException>(() => new InMemoryMuleDiagnostics(new InMemoryMuleStore(), Options.Create(new MuleSettings()), null));

        var positiveLock = new InMemoryMuleDiagnostics(
            new InMemoryMuleStore(),
            Options.Create(new MuleSettings { LockTimeout = TimeSpan.FromSeconds(1) }),
            new MuleRuntimeMetrics());
        Assert.NotNull(await positiveLock.GetSnapshotAsync());

        var defaultLock = new InMemoryMuleDiagnostics(
            new InMemoryMuleStore(),
            Options.Create(new MuleSettings { LockTimeout = TimeSpan.Zero }),
            new MuleRuntimeMetrics());
        Assert.NotNull(await defaultLock.GetSnapshotAsync());
    }

    private static DurableAction CreateAction(
        string value,
        DateTimeOffset createdOnUtc,
        DurableActionStatus status = DurableActionStatus.Pending,
        DateTimeOffset? lockedOnUtc = null,
        DateTimeOffset? completedOnUtc = null,
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
            CompletedOnUtc = completedOnUtc,
            TerminalOnUtc = completedOnUtc,
            NextAttemptOnUtc = nextAttemptOnUtc
        };
}
