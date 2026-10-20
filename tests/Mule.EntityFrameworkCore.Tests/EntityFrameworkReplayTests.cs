namespace Mule.EntityFrameworkCore.Tests;

using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule.EntityFrameworkCore;

public sealed class EntityFrameworkReplayTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.replay.ef.v1");

    [Fact]
    public async Task Replay_Model_Should_Be_Automatic_And_Host_Configurable()
    {
        using var host = CreateHost(out var path, options => options.ConfigureReplays(entity =>
        {
            entity.ToTable("CustomMuleReplays");
            entity.Property(x => x.RequestedBy).HasMaxLength(64);
        }));
        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<ReplayDbContext>();
        await db.Database.EnsureCreatedAsync();
        var entity = db.Model.FindEntityType(typeof(MuleReplay));

        Assert.NotNull(entity);
        Assert.Equal("CustomMuleReplays", entity.GetTableName());
        Assert.Equal(64, entity.FindProperty(nameof(MuleReplay.RequestedBy)).GetMaxLength());
        Assert.True(entity.FindProperty(nameof(MuleReplay.ConcurrencyToken)).IsConcurrencyToken);
        Assert.Contains(entity.GetIndexes(), x => x.IsUnique && x.Properties.Single().Name == nameof(MuleReplay.ReplayActionId));
        Assert.Throws<ArgumentNullException>(() => new MuleEntityFrameworkOptions().ConfigureReplays(null));

        TryDelete(path);
    }

    [Fact]
    public async Task Replay_Storage_Should_Schedule_Claim_Materialize_And_Query()
    {
        using var host = CreateHost(out var path);
        var source = await SeedAsync(host, DurableActionStatus.Failed);
        var now = DateTimeOffset.UtcNow;
        var request = Request(now);
        MuleReplayResult scheduled;

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            scheduled = await storage.ScheduleAsync(source.Id, request, now);
            Assert.Equal(MuleReplayOutcome.AlreadyScheduled, (await storage.ScheduleAsync(source.Id, request, now)).Outcome);
            Assert.Equal(MuleReplayOutcome.RequestConflict, (await storage.ScheduleAsync(source.Id, Request(now, request.RequestId, "changed"), now)).Outcome);
            Assert.Equal(MuleReplayOutcome.ReplayAlreadyExists, (await storage.ScheduleAsync(source.Id, Request(now), now)).Outcome);
            Assert.Equal(MuleReplayOutcome.NotFound, (await storage.ScheduleAsync(Ulid.NewUlid(), Request(now), now)).Outcome);
        }

        MuleReplayClaim claim;
        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            claim = Assert.Single(await storage.ClaimScheduledAsync(10, TimeSpan.FromMinutes(1), now));
        }

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            var action = Mule.Replay.MuleReplayModel.CreateAction(claim, now);
            Assert.Equal(MuleReplayMaterializationOutcome.Created, await storage.MaterializeAsync(claim, action, now));
        }

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            var info = await storage.FindAsync(request.RequestId);
            Assert.Equal(MuleReplayStatus.Materialized, info.Status);
            Assert.Equal(source.Id, info.ReplayRootActionId);
            Assert.Equal(1, info.ReplaySequence);
            Assert.Null(await storage.GetNextScheduledOnUtcAsync(TimeSpan.FromMinutes(1), now));

            var db = scope.ServiceProvider.GetRequiredService<ReplayDbContext>();
            var original = await db.Set<DurableAction>().AsNoTracking().SingleAsync(x => x.Id == source.Id);
            var replayed = await db.Set<DurableAction>().AsNoTracking().SingleAsync(x => x.Id == scheduled.ReplayActionId);
            Assert.Equal(DurableActionStatus.Failed, original.Status);
            Assert.Equal(DurableActionStatus.Pending, replayed.Status);
            Assert.Equal(0, replayed.Attempts);
            Assert.NotEqual(original.DeduplicationKey, replayed.DeduplicationKey);
        }

        TryDelete(path);
    }

    [Fact]
    public async Task Replay_Claims_Should_Coordinate_Replicas_And_Expired_Leases()
    {
        using var host = CreateHost(out var path);
        var source = await SeedAsync(host, DurableActionStatus.Failed);
        var now = DateTimeOffset.UtcNow;
        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            await storage.ScheduleAsync(source.Id, Request(now), now);
        }

        MuleReplayClaim firstClaim;
        using (var firstScope = host.Services.CreateScope())
        using (var secondScope = host.Services.CreateScope())
        {
            var first = firstScope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            var second = secondScope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            firstClaim = Assert.Single(await first.ClaimScheduledAsync(1, TimeSpan.FromMinutes(1), now));
            Assert.Empty(await second.ClaimScheduledAsync(1, TimeSpan.FromMinutes(1), now));
            Assert.Equal(now.AddMinutes(1), await second.GetNextScheduledOnUtcAsync(TimeSpan.FromMinutes(1), now));
        }

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            await storage.ReleaseAsync(firstClaim, "retry", now.AddSeconds(5));
            Assert.Empty(await storage.ClaimScheduledAsync(1, TimeSpan.FromMinutes(1), now));
            Assert.Equal(now.AddSeconds(5), await storage.GetNextScheduledOnUtcAsync(TimeSpan.FromMinutes(1), now));
            Assert.Single(await storage.ClaimScheduledAsync(1, TimeSpan.FromMinutes(1), now.AddSeconds(5)));
        }

        TryDelete(path);
    }

    [Fact]
    public async Task Materialization_Should_Reject_Stale_Lease_And_Reconcile_Existing_Action()
    {
        using var host = CreateHost(out var path);
        var source = await SeedAsync(host, DurableActionStatus.Failed);
        var now = DateTimeOffset.UtcNow;
        MuleReplayClaim claim;
        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            await storage.ScheduleAsync(source.Id, Request(now), now);
            claim = Assert.Single(await storage.ClaimScheduledAsync(1, TimeSpan.FromMinutes(1), now));
        }

        var action = Mule.Replay.MuleReplayModel.CreateAction(claim, now);
        var stale = new MuleReplayClaim
        {
            Replay = claim.Replay,
            SourceAction = claim.SourceAction,
            LockToken = Ulid.NewUlid()
        };
        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            Assert.Equal(MuleReplayMaterializationOutcome.LeaseLost, await storage.MaterializeAsync(stale, action, now));
            await Assert.ThrowsAsync<ArgumentNullException>(() => storage.MaterializeAsync(null, action, now));
            await Assert.ThrowsAsync<ArgumentNullException>(() => storage.MaterializeAsync(claim, null, now));
            await Assert.ThrowsAsync<ArgumentNullException>(() => storage.ReleaseAsync(null, "error", now));
        }

        var outcomes = await Task.WhenAll(Enumerable.Range(0, 2).Select(async _ =>
        {
            using var scope = host.Services.CreateScope();
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            var candidate = Mule.Replay.MuleReplayModel.CreateAction(claim, now);
            return await storage.MaterializeAsync(claim, candidate, now);
        }));

        Assert.Contains(MuleReplayMaterializationOutcome.Created, outcomes);
        Assert.Contains(MuleReplayMaterializationOutcome.Reconciled, outcomes);

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            var missingReplay = new MuleReplayClaim
            {
                Replay = new MuleReplay { RequestId = Ulid.NewUlid() },
                SourceAction = claim.SourceAction,
                LockToken = Ulid.NewUlid()
            };
            Assert.Equal(MuleReplayMaterializationOutcome.LeaseLost, await storage.MaterializeAsync(missingReplay, action, now));
            await storage.ReleaseAsync(missingReplay, "ignored", now);
            await storage.ReleaseAsync(stale, "ignored", now);
        }

        TryDelete(path);
    }

    [Fact]
    public async Task NonFailed_Action_Should_Not_Be_Eligible()
    {
        using var host = CreateHost(out var path);
        var source = await SeedAsync(host, DurableActionStatus.Completed);
        using var scope = host.Services.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();

        var result = await storage.ScheduleAsync(source.Id, Request(DateTimeOffset.UtcNow), DateTimeOffset.UtcNow);

        Assert.Equal(MuleReplayOutcome.NotEligible, result.Outcome);
        TryDelete(path);
    }

    [Fact]
    public async Task Concurrent_Schedules_Should_Reconcile_Unique_Constraint_Races()
    {
        using var host = CreateHost(out var path);
        var source = await SeedAsync(host, DurableActionStatus.Failed);
        var now = DateTimeOffset.UtcNow;
        var request = Request(now);

        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(async _ =>
        {
            using var scope = host.Services.CreateScope();
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            return await storage.ScheduleAsync(source.Id, request, now);
        }));

        Assert.Single(results.Where(x => x.Outcome == MuleReplayOutcome.Scheduled));
        Assert.Equal(7, results.Count(x => x.Outcome == MuleReplayOutcome.AlreadyScheduled));
        Assert.Single(results.Select(x => x.ReplayActionId).Distinct());
        TryDelete(path);
    }

    [Fact]
    public async Task Claim_Should_Ignore_Replay_When_Source_No_Longer_Exists()
    {
        using var host = CreateHost(out var path);
        var now = DateTimeOffset.UtcNow;
        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<ReplayDbContext>();
            await db.Database.EnsureCreatedAsync();
            db.Set<MuleReplay>().Add(new MuleReplay
            {
                RequestId = Ulid.NewUlid(),
                SourceActionId = Ulid.NewUlid(),
                ReplayRootActionId = Ulid.NewUlid(),
                ReplayActionId = Ulid.NewUlid(),
                ActionKey = Key,
                Lane = MuleSettings.DefaultLane,
                RequestedOnUtc = now,
                ScheduleOnUtc = now
            });
            await db.SaveChangesAsync();
        }

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            Assert.Empty(await storage.ClaimScheduledAsync(1, TimeSpan.FromMinutes(1), now));
        }

        TryDelete(path);
    }

    [Fact]
    public async Task Replay_Storage_Should_Preserve_Chain_Root_And_Increment_Sequence()
    {
        using var host = CreateHost(out var path);
        var original = await SeedAsync(host, DurableActionStatus.Failed);
        var now = DateTimeOffset.UtcNow;
        MuleReplayResult first;

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            first = await storage.ScheduleAsync(original.Id, Request(now), now);
            var claim = Assert.Single(await storage.ClaimScheduledAsync(1, TimeSpan.FromMinutes(1), now));
            var action = Mule.Replay.MuleReplayModel.CreateAction(claim, now);
            await storage.MaterializeAsync(claim, action, now);

            var db = scope.ServiceProvider.GetRequiredService<ReplayDbContext>();
            action.Status = DurableActionStatus.Failed;
            action.TerminalOnUtc = now;
            db.Set<DurableAction>().Update(action);
            await db.SaveChangesAsync();
        }

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            var second = await storage.ScheduleAsync(first.ReplayActionId, Request(now), now);

            Assert.Equal(MuleReplayOutcome.Scheduled, second.Outcome);
            Assert.Equal(original.Id, second.Replay.ReplayRootActionId);
            Assert.Equal(2, second.Replay.ReplaySequence);
        }

        TryDelete(path);
    }

    [Fact]
    public async Task Replay_Storage_Should_Filter_Every_Scheduling_Boundary()
    {
        using var host = CreateHost(out var path);
        var source = await SeedAsync(host, DurableActionStatus.Failed);
        var now = DateTimeOffset.UtcNow;

        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<ReplayDbContext>();
            db.Set<MuleReplay>().AddRange(
                Replay(source, now, nextAttemptOnUtc: now.AddSeconds(-1)),
                Replay(source, now, lockedOnUtc: now.AddMinutes(-2)),
                Replay(source, now.AddMinutes(1)),
                Replay(source, now, nextAttemptOnUtc: now.AddMinutes(1)),
                Replay(source, now, lockedOnUtc: now));
            await db.SaveChangesAsync();
        }

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            var claims = await storage.ClaimScheduledAsync(10, TimeSpan.FromMinutes(1), now);
            Assert.Equal(2, claims.Count);
        }

        TryDelete(path);
    }

    [Fact]
    public async Task Replay_Storage_Should_Defend_Against_Stale_Private_Candidates()
    {
        using var host = CreateHost(out var path);
        var source = await SeedAsync(host, DurableActionStatus.Failed);
        var now = DateTimeOffset.UtcNow;

        using var scope = host.Services.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
        var missing = await InvokePrivateAsync<MuleReplayClaim>(
            storage,
            "TryClaimAsync",
            Ulid.NewUlid(),
            now.AddMinutes(-1),
            now,
            CancellationToken.None);
        Assert.Null(missing);

        var future = Replay(source, now.AddMinutes(1));
        var db = scope.ServiceProvider.GetRequiredService<ReplayDbContext>();
        db.Set<MuleReplay>().Add(future);
        await db.SaveChangesAsync();
        var stale = await InvokePrivateAsync<MuleReplayClaim>(
            storage,
            "TryClaimAsync",
            future.RequestId,
            now.AddMinutes(-1),
            now,
            CancellationToken.None);
        Assert.Null(stale);

        TryDelete(path);
    }

    [Fact]
    public async Task Replay_Storage_Should_Evaluate_All_Claim_And_Due_Boundaries()
    {
        using var host = CreateHost(out var path);
        using var scope = host.Services.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
        var type = storage.GetType();
        var canClaim = type.GetMethod("CanClaimReplay", BindingFlags.Static | BindingFlags.NonPublic);
        var nextDue = type.GetMethod("GetNextReplayOnUtc", BindingFlags.Static | BindingFlags.NonPublic);
        var now = DateTimeOffset.UtcNow;
        var expiration = now.AddMinutes(-1);

        Assert.True((bool)canClaim.Invoke(null, [Replay(null, now), expiration, now]));
        Assert.False((bool)canClaim.Invoke(null, [Replay(null, now, status: MuleReplayStatus.Materialized), expiration, now]));
        Assert.False((bool)canClaim.Invoke(null, [Replay(null, now.AddSeconds(1)), expiration, now]));
        Assert.False((bool)canClaim.Invoke(null, [Replay(null, now, nextAttemptOnUtc: now.AddSeconds(1)), expiration, now]));
        Assert.False((bool)canClaim.Invoke(null, [Replay(null, now, lockedOnUtc: now), expiration, now]));
        Assert.True((bool)canClaim.Invoke(null, [Replay(null, now, nextAttemptOnUtc: now, lockedOnUtc: expiration), expiration, now]));

        Assert.Equal(now, (DateTimeOffset)nextDue.Invoke(null, [now.AddSeconds(-1), null, null, expiration, TimeSpan.FromMinutes(1), now]));
        Assert.Equal(now.AddSeconds(1), (DateTimeOffset)nextDue.Invoke(null, [now.AddSeconds(1), null, null, expiration, TimeSpan.FromMinutes(1), now]));

        TryDelete(path);
        await Task.CompletedTask;
    }

    private static IHost CreateHost(
        out string databasePath,
        Action<MuleEntityFrameworkOptions> configure = null)
    {
        databasePath = Path.Combine(Path.GetTempPath(), $"mule-replay-{Guid.NewGuid():N}.db");
        var capturedPath = databasePath;
        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddDbContext<ReplayDbContext>(options => options.UseSqlite($"Data Source={capturedPath}"));
                services.AddMule(mule => mule.UseEntityFrameworkCore<ReplayDbContext>(configure));
            })
            .Build();
    }

    private static async Task<DurableAction> SeedAsync(IHost host, DurableActionStatus status)
    {
        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<ReplayDbContext>();
        await db.Database.EnsureCreatedAsync();
        var action = new DurableAction
        {
            Key = Key,
            Lane = "replay",
            Payload = "{\"value\":\"stored\"}",
            PayloadType = typeof(Dictionary<string, string>).AssemblyQualifiedName,
            Metadata = "{\"original\":\"yes\"}",
            CorrelationId = "correlation",
            DeduplicationKey = "original",
            Status = status,
            CreatedOnUtc = DateTimeOffset.UtcNow,
            TerminalOnUtc = status == DurableActionStatus.Failed ? DateTimeOffset.UtcNow : null
        };
        db.Set<DurableAction>().Add(action);
        await db.SaveChangesAsync();
        return action;
    }

    private static MuleReplayRequest Request(DateTimeOffset now, Ulid? id = null, string reason = "fixed")
        => new()
        {
            RequestId = id ?? Ulid.NewUlid(),
            RequestedBy = "support",
            Reason = reason,
            ScheduleOnUtc = now,
            Metadata = new Dictionary<string, string> { ["replay"] = "yes" }
        };

    private static MuleReplay Replay(
        DurableAction source,
        DateTimeOffset scheduleOnUtc,
        DateTimeOffset? nextAttemptOnUtc = null,
        DateTimeOffset? lockedOnUtc = null,
        MuleReplayStatus status = MuleReplayStatus.Scheduled)
        => new()
        {
            RequestId = Ulid.NewUlid(),
            ConcurrencyToken = Ulid.NewUlid(),
            SourceActionId = source?.Id ?? Ulid.NewUlid(),
            ReplayRootActionId = source?.Id ?? Ulid.NewUlid(),
            ReplayActionId = Ulid.NewUlid(),
            BranchId = Ulid.NewUlid(),
            ActionKey = source?.Key ?? Key,
            Lane = source?.Lane ?? MuleSettings.DefaultLane,
            Status = status,
            RequestedOnUtc = scheduleOnUtc,
            ScheduleOnUtc = scheduleOnUtc,
            NextAttemptOnUtc = nextAttemptOnUtc,
            LockedOnUtc = lockedOnUtc,
            LockToken = lockedOnUtc == null ? Ulid.Empty : Ulid.NewUlid()
        };

    private static async Task<T> InvokePrivateAsync<T>(object target, string methodName, params object[] arguments)
    {
        var method = target.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic);
        return await (Task<T>)method.Invoke(target, arguments);
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

    private sealed class ReplayDbContext : DbContext
    {
        public ReplayDbContext(DbContextOptions<ReplayDbContext> options)
            : base(options)
        {
        }
    }
}
