namespace Mule.EntityFrameworkCore.Tests;

using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule.Diagnostics;
using Mule.EntityFrameworkCore;

public sealed class EntityFrameworkMuleTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.ef.capture.v1");

    [Fact]
    public async Task UseEntityFrameworkCore_Should_Add_Mule_Entity_To_App_DbContext_Model()
    {
        using var host = CreateAppDbContextHost(out var databasePath);

        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();

        Assert.NotNull(db.Model.FindEntityType(typeof(DurableAction)));

        TryDelete(databasePath);
    }

    [Fact]
    public async Task UseEntityFrameworkCore_Should_Apply_Mule_Model_Configuration()
    {
        using var host = CreateAppDbContextHost(
            out var databasePath,
            ef => ef.ConfigureActions(entity =>
            {
                entity.Property(x => x.PayloadType).HasMaxLength(128);
                entity.Property(x => x.CorrelationId).HasMaxLength(64);
            }));

        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var entity = db.Model.FindEntityType(typeof(DurableAction));

        Assert.Equal(128, entity.FindProperty(nameof(DurableAction.PayloadType)).GetMaxLength());
        Assert.Equal(64, entity.FindProperty(nameof(DurableAction.CorrelationId)).GetMaxLength());
        Assert.True(entity.FindProperty(nameof(DurableAction.ConcurrencyToken)).IsConcurrencyToken);

        TryDelete(databasePath);
    }

    [Fact]
    public async Task UseEntityFrameworkCore_Should_Persist_Action_In_App_DbContext_Model()
    {
        using var host = CreateAppDbContextHost(out var databasePath);

        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            await db.Database.EnsureCreatedAsync();
        }

        using (var scope = host.Services.CreateScope())
        {
            var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
            await client.EnqueueAsync(Key, new TestPayload("stored"));
        }

        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var action = await db.Set<DurableAction>().AsNoTracking().SingleAsync();

            Assert.Equal(Key, action.Key);
            Assert.Equal(DurableActionStatus.Pending, action.Status);
        }

        TryDelete(databasePath);
    }

    [Fact]
    public async Task LockAsync_Should_Allow_Only_One_Replica_To_Claim_Action()
    {
        using var host = CreateAppDbContextHost(out var databasePath);
        var actionId = Ulid.NewUlid();

        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Set<DurableAction>().Add(new DurableAction
            {
                Id = actionId,
                Key = Key,
                Payload = "{}",
                PayloadType = typeof(TestPayload).AssemblyQualifiedName,
                Status = DurableActionStatus.Pending,
                CreatedOnUtc = DateTimeOffset.UtcNow
            });
            await db.SaveChangesAsync();
        }

        using var firstScope = host.Services.CreateScope();
        using var secondScope = host.Services.CreateScope();
        var firstStorage = firstScope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var secondStorage = secondScope.ServiceProvider.GetRequiredService<IMuleStorage>();

        var firstClaim = await firstStorage.LockAsync(actionId, TimeSpan.FromMinutes(5), DateTimeOffset.UtcNow);
        var secondClaim = await secondStorage.LockAsync(actionId, TimeSpan.FromMinutes(5), DateTimeOffset.UtcNow);

        Assert.NotNull(firstClaim);
        Assert.Null(secondClaim);

        TryDelete(databasePath);
    }

    [Fact]
    public async Task LockAsync_Should_Update_Concurrency_Token()
    {
        using var host = CreateAppDbContextHost(out var databasePath);
        var actionId = Ulid.NewUlid();
        var originalToken = Ulid.NewUlid();

        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Set<DurableAction>().Add(new DurableAction
            {
                Id = actionId,
                ConcurrencyToken = originalToken,
                Key = Key,
                Payload = "{}",
                PayloadType = typeof(TestPayload).AssemblyQualifiedName,
                Status = DurableActionStatus.Pending,
                CreatedOnUtc = DateTimeOffset.UtcNow
            });
            await db.SaveChangesAsync();
        }

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
            var action = await storage.LockAsync(actionId, TimeSpan.FromMinutes(5), DateTimeOffset.UtcNow);

            Assert.NotNull(action);
            Assert.NotEqual(originalToken, action.ConcurrencyToken);
        }

        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            var action = await db.Set<DurableAction>().AsNoTracking().SingleAsync();

            Assert.Equal(DurableActionStatus.Locked, action.Status);
            Assert.NotEqual(originalToken, action.ConcurrencyToken);
        }

        TryDelete(databasePath);
    }

    [Fact]
    public async Task EnqueueAsync_Should_Persist_Action()
    {
        using var host = CreateHost(out var databasePath);
        await EnsureDatabaseAsync(host);

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();

        await client.EnqueueAsync(Key, new TestPayload("stored"));

        using var verificationScope = host.Services.CreateScope();
        var db = verificationScope.ServiceProvider.GetRequiredService<MuleDbContext>();
        var action = await db.Actions.AsNoTracking().SingleAsync();

        Assert.Equal(Key, action.Key);
        Assert.Equal(DurableActionStatus.Pending, action.Status);

        TryDelete(databasePath);
    }

    [Fact]
    public async Task EnqueueManyAsync_Should_Persist_Actions()
    {
        using var host = CreateHost(out var databasePath);
        await EnsureDatabaseAsync(host);

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();

        var ids = await client.EnqueueManyAsync(Enumerable.Range(0, 10)
            .Select(index => MuleIntent.For(
                Key,
                new TestPayload($"stored-{index}"),
                options => options.CorrelationId = $"batch-{index}")));

        using var verificationScope = host.Services.CreateScope();
        var db = verificationScope.ServiceProvider.GetRequiredService<MuleDbContext>();
        var actions = await db.Actions.AsNoTracking().OrderBy(x => x.CorrelationId).ToArrayAsync();

        Assert.Equal(10, ids.Count);
        Assert.Equal(10, ids.Distinct().Count());
        Assert.Equal(10, actions.Length);
        Assert.Equal("batch-0", actions[0].CorrelationId);

        TryDelete(databasePath);
    }

    [Fact]
    public async Task CleanCompletedAsync_Should_Delete_Completed_Actions_In_Configured_Batch()
    {
        using var host = CreateHost(out var databasePath);
        await EnsureDatabaseAsync(host);

        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
            db.Actions.AddRange(Enumerable.Range(0, 3).Select(index => new DurableAction
            {
                Key = Key,
                Payload = "{}",
                PayloadType = typeof(TestPayload).AssemblyQualifiedName,
                Status = DurableActionStatus.Completed,
                CompletedOnUtc = DateTimeOffset.UtcNow.AddMinutes(-10),
                CreatedOnUtc = DateTimeOffset.UtcNow.AddTicks(index)
            }));
            await db.SaveChangesAsync();
        }

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
            var deleted = await storage.CleanCompletedAsync(DateTimeOffset.UtcNow, 2);
            await storage.SaveChangesAsync();

            Assert.Equal(2, deleted);
        }

        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();

            Assert.Equal(1, await db.Actions.CountAsync());
        }

        TryDelete(databasePath);
    }

    [Fact]
    public async Task Diagnostics_Should_Report_State_Counts()
    {
        using var host = CreateHost(out var databasePath);
        await EnsureDatabaseAsync(host);

        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
        db.Actions.Add(new DurableAction
        {
            Key = Key,
            Payload = "{}",
            PayloadType = typeof(TestPayload).AssemblyQualifiedName,
            Status = DurableActionStatus.Failed,
            LastError = "failed"
        });
        await db.SaveChangesAsync();

        var diagnostics = scope.ServiceProvider.GetRequiredService<IMuleDiagnostics>();
        var snapshot = await diagnostics.GetSnapshotAsync();

        Assert.Equal(1, snapshot.Failed);
        Assert.NotNull(snapshot.OldestFailedOnUtc);

        TryDelete(databasePath);
    }

    [Fact]
    public async Task HostedService_Should_Execute_Persisted_Action()
    {
        using var host = CreateHost(out var databasePath);
        await EnsureDatabaseAsync(host);
        await host.StartAsync();

        using var scope = host.Services.CreateScope();
        var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
        await client.EnqueueAsync(Key, new TestPayload("hello"));

        var probe = host.Services.GetRequiredService<TestProbe>();
        await WaitForActionStatusAsync(host, DurableActionStatus.Completed);
        await host.StopAsync();

        Assert.Equal("hello", Assert.Single(probe.Values));

        using var verificationScope = host.Services.CreateScope();
        var db = verificationScope.ServiceProvider.GetRequiredService<MuleDbContext>();
        var action = await db.Actions.AsNoTracking().SingleAsync();
        Assert.Equal(DurableActionStatus.Completed, action.Status);

        TryDelete(databasePath);
    }

    [Fact]
    public async Task Deferral_And_State_Projection_Should_Enforce_Concurrency_Tokens()
    {
        using var host = CreateHost(out var databasePath);
        await EnsureDatabaseAsync(host);
        var now = DateTimeOffset.UtcNow;
        Ulid actionId;
        DurableAction firstClaim;

        using (var scope = host.Services.CreateScope())
        {
            var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
            actionId = await client.EnqueueAsync(
                Key,
                new TestPayload("defer"),
                options => options.DeduplicationKey = "ef-deferral");
            var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
            firstClaim = await storage.LockAsync(actionId, TimeSpan.FromMinutes(1), now);
            var execution = Assert.IsAssignableFrom<IMuleExecutionStorage>(storage);

            Assert.Equal(
                MuleActionTransitionOutcome.Applied,
                await execution.DeferClaimAsync(
                    actionId,
                    firstClaim.ConcurrencyToken,
                    now,
                    now.AddMinutes(2),
                    DeferralReason.From("lease-busy")));
        }

        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
            var deferred = await db.Actions.AsNoTracking().SingleAsync(x => x.Id == actionId);
            Assert.Equal(DurableActionStatus.Pending, deferred.Status);
            Assert.Equal(DurableActionPendingReason.Deferred, deferred.PendingReason);
            Assert.Equal("lease-busy", deferred.DeferralReasonCode);
            Assert.Equal(0, deferred.Attempts);
        }

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
            var execution = Assert.IsAssignableFrom<IMuleExecutionStorage>(storage);
            var secondClaim = await storage.LockAsync(actionId, TimeSpan.FromMinutes(1), now.AddMinutes(3));

            Assert.Equal(
                MuleActionTransitionOutcome.LeaseLost,
                await execution.CompleteClaimAsync(actionId, firstClaim.ConcurrencyToken, now.AddMinutes(3)));
            Assert.Equal(
                MuleActionTransitionOutcome.Applied,
                await execution.CompleteClaimAsync(actionId, secondClaim.ConcurrencyToken, now.AddMinutes(3)));
        }

        using (var scope = host.Services.CreateScope())
        {
            var projectionStorage = Assert.IsAssignableFrom<IMuleStateProjectionStorage>(
                scope.ServiceProvider.GetRequiredService<IMuleStorage>());
            var projected = new DurableAction
            {
                Key = Key,
                Lane = MuleSettings.DefaultLane,
                Payload = "{}",
                PayloadType = typeof(TestPayload).AssemblyQualifiedName,
                Status = DurableActionStatus.Pending,
                PendingReason = DurableActionPendingReason.Deferred,
                DeferralReasonCode = "projection",
                NextAttemptOnUtc = now.AddMinutes(5),
                CreatedOnUtc = now
            };
            var inserted = Assert.Single(await projectionStorage.ProjectStateRangeAsync(
                [new MuleActionStateProjection(projected, null)]));
            Assert.Equal(MuleActionTransitionOutcome.Applied, inserted.Outcome);

            var previousToken = projected.ConcurrencyToken;
            projected.ConcurrencyToken = Ulid.NewUlid();
            projected.PendingReason = DurableActionPendingReason.Retry;
            projected.DeferralReasonCode = null;
            var updated = Assert.Single(await projectionStorage.ProjectStateRangeAsync(
                [new MuleActionStateProjection(projected, previousToken)]));
            Assert.Equal(MuleActionTransitionOutcome.Applied, updated.Outcome);

            projected.ConcurrencyToken = Ulid.NewUlid();
            var stale = Assert.Single(await projectionStorage.ProjectStateRangeAsync(
                [new MuleActionStateProjection(projected, previousToken)]));
            Assert.Equal(MuleActionTransitionOutcome.LeaseLost, stale.Outcome);
        }

        TryDelete(databasePath);
    }

    [Fact]
    public async Task Execution_Storage_Should_Cover_Candidates_Claim_Outcomes_And_Projection_Validation()
    {
        using var host = CreateHost(out var databasePath);
        await EnsureDatabaseAsync(host);
        var now = DateTimeOffset.UtcNow;
        var due = new DurableAction
        {
            Key = Key,
            Lane = "coverage",
            Payload = "{}",
            PayloadType = typeof(TestPayload).AssemblyQualifiedName,
            CreatedOnUtc = now.AddMinutes(-2)
        };
        var future = new DurableAction
        {
            Key = Key,
            Lane = "coverage",
            Payload = "{}",
            PayloadType = typeof(TestPayload).AssemblyQualifiedName,
            CreatedOnUtc = now,
            NextAttemptOnUtc = now.AddMinutes(2)
        };

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
            await storage.AddRangeAsync([due, future]);
            await storage.SaveChangesAsync();
        }

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
            var execution = Assert.IsAssignableFrom<IMuleExecutionStorage>(storage);
            var candidates = await execution.ReadPendingCandidatesAsync(
                "coverage", -1, 10, TimeSpan.FromMinutes(1), now);
            Assert.Contains(candidates, x => x.Id == due.Id);
            Assert.DoesNotContain(candidates, x => x.Id == future.Id);
            Assert.Null(await execution.FindActionAsync(Ulid.NewUlid()));
            Assert.Equal(due.Id, (await execution.FindActionAsync(due.Id)).Id);

            Assert.Equal(MuleActionTransitionOutcome.NotFound,
                await execution.CompleteClaimAsync(Ulid.NewUlid(), Ulid.NewUlid(), now));
            var claim = await storage.LockAsync(due.Id, TimeSpan.FromMinutes(1), now);
            Assert.Equal(MuleActionTransitionOutcome.LeaseLost,
                await execution.FailClaimAsync(due.Id, Ulid.NewUlid(), "stale", now, null));
            Assert.Equal(MuleActionTransitionOutcome.Applied,
                await execution.FailClaimAsync(due.Id, claim.ConcurrencyToken, "retry", now, now.AddSeconds(1)));

            Assert.Equal(MuleActionTransitionOutcome.NotFound,
                await execution.DeferClaimAsync(
                    Ulid.NewUlid(), Ulid.NewUlid(), now, now, DeferralReason.From("missing")));
        }

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
            var execution = Assert.IsAssignableFrom<IMuleExecutionStorage>(storage);
            var claim = await storage.LockAsync(due.Id, TimeSpan.FromMinutes(1), now.AddMinutes(1));
            Assert.Equal(MuleActionTransitionOutcome.Applied,
                await execution.FailClaimAsync(due.Id, claim.ConcurrencyToken, "terminal", now.AddMinutes(1), null));

            var projectionStorage = Assert.IsAssignableFrom<IMuleStateProjectionStorage>(storage);
            await Assert.ThrowsAsync<ArgumentNullException>(() => projectionStorage.ProjectStateRangeAsync(null));
            await Assert.ThrowsAsync<ArgumentException>(() =>
                projectionStorage.ProjectStateRangeAsync([null]));
            await Assert.ThrowsAsync<ArgumentException>(() =>
                projectionStorage.ProjectStateRangeAsync([
                    new MuleActionStateProjection(future, future.ConcurrencyToken),
                    new MuleActionStateProjection(future, future.ConcurrencyToken)]));
            Assert.Empty(await projectionStorage.ProjectStateRangeAsync([]));

            var missing = new DurableAction
            {
                Key = Key,
                Payload = "{}",
                PayloadType = typeof(TestPayload).AssemblyQualifiedName
            };
            Assert.Equal(MuleActionTransitionOutcome.NotFound,
                Assert.Single(await projectionStorage.ProjectStateRangeAsync([
                    new MuleActionStateProjection(missing, Ulid.NewUlid())])).Outcome);

            var persistedFuture = await execution.FindActionAsync(future.Id);
            Assert.Equal(MuleActionTransitionOutcome.Applied,
                Assert.Single(await projectionStorage.ProjectStateRangeAsync([
                    new MuleActionStateProjection(persistedFuture, persistedFuture.ConcurrencyToken)])).Outcome);
            persistedFuture.ConcurrencyToken = Ulid.NewUlid();
            Assert.Equal(MuleActionTransitionOutcome.LeaseLost,
                Assert.Single(await projectionStorage.ProjectStateRangeAsync([
                    new MuleActionStateProjection(persistedFuture, null)])).Outcome);
        }

        TryDelete(databasePath);
    }

    private static IHost CreateHost(out string databasePath)
    {
        databasePath = Path.Combine(Path.GetTempPath(), $"mule-{Guid.NewGuid():N}.db");
        var capturedPath = databasePath;

        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.Configure<MuleSettings>(settings =>
                {
                    settings.DispatchInterval = TimeSpan.FromMilliseconds(50);
                    settings.RetryDelay = TimeSpan.FromMilliseconds(50);
                    settings.MaxAttempts = 1;
                });

                services.AddSingleton<TestProbe>();
                services.AddMule(mule =>
                {
                    mule.AddActionsFromAssemblyContaining<CaptureTestPayloadAction>();
                });
                services.UseEntityFrameworkMule(options => options.UseSqlite($"Data Source={capturedPath}"));
            })
            .Build();
    }

    private static IHost CreateAppDbContextHost(
        out string databasePath,
        Action<MuleEntityFrameworkOptions> configureMule = null)
    {
        databasePath = Path.Combine(Path.GetTempPath(), $"mule-app-{Guid.NewGuid():N}.db");
        var capturedPath = databasePath;

        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddDbContext<TestDbContext>(options => options.UseSqlite($"Data Source={capturedPath}"));
                services.AddMule(mule => mule
                    .UseEntityFrameworkCore<TestDbContext>(configureMule)
                    .AddActionsFromAssemblyContaining<CaptureTestPayloadAction>());
            })
            .Build();
    }

    private static async Task EnsureDatabaseAsync(IHost host)
    {
        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
        await db.Database.EnsureCreatedAsync();
    }

    private static async Task WaitForActionStatusAsync(IHost host, DurableActionStatus status)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        while (!timeout.IsCancellationRequested)
        {
            using var scope = host.Services.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
            var current = await db.Actions.AsNoTracking().Select(x => x.Status).SingleOrDefaultAsync(timeout.Token);

            if (current == status)
                return;

            await Task.Delay(25, timeout.Token);
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
        }
    }

    private sealed record TestPayload(string Value);

    [MuleAction("tests.ef.capture.v1")]
    private sealed class CaptureTestPayloadAction : IMuleAction<TestPayload>
    {
        private readonly TestProbe _probe;

        public CaptureTestPayloadAction(TestProbe probe)
        {
            _probe = probe;
        }

        public ValueTask ExecuteAsync(MuleActionContext<TestPayload> context, CancellationToken cancellationToken)
        {
            _probe.Record(context.Payload.Value);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestProbe
    {
        private readonly ConcurrentQueue<string> _values = new();

        public IReadOnlyCollection<string> Values => _values.ToArray();

        public void Record(string value) => _values.Enqueue(value);
    }

    private sealed class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions<TestDbContext> options)
            : base(options)
        {
        }
    }
}
