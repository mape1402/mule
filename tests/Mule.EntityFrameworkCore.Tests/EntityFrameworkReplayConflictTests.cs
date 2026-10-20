namespace Mule.EntityFrameworkCore.Tests;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule.EntityFrameworkCore;

public sealed class EntityFrameworkReplayConflictTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.replay.ef.conflict.v1");

    [Theory]
    [InlineData(ConflictMode.ScheduleRequest, MuleReplayOutcome.AlreadyScheduled)]
    [InlineData(ConflictMode.ScheduleBranch, MuleReplayOutcome.ReplayAlreadyExists)]
    public async Task Schedule_Should_Reconcile_Concurrent_Insert(
        ConflictMode mode,
        MuleReplayOutcome expected)
    {
        using var host = CreateHost(mode, out var path);
        var source = await SeedSourceAsync(host);
        var now = DateTimeOffset.UtcNow;
        using var scope = host.Services.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();

        var result = await storage.ScheduleAsync(source.Id, Request(now), now);

        Assert.Equal(expected, result.Outcome);
        TryDelete(path);
    }

    [Fact]
    public async Task Schedule_Should_Propagate_Unrecognized_Update_Error()
    {
        using var host = CreateHost(ConflictMode.ScheduleFailure, out var path);
        var source = await SeedSourceAsync(host);
        var now = DateTimeOffset.UtcNow;
        using var scope = host.Services.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();

        await Assert.ThrowsAsync<DbUpdateException>(() => storage.ScheduleAsync(source.Id, Request(now), now));
        TryDelete(path);
    }

    [Fact]
    public async Task Materialize_Should_Reconcile_Action_And_Replay_Conflicts()
    {
        using var host = CreateHost(ConflictMode.Materialize, out var path);
        var source = await SeedSourceAsync(host);
        var now = DateTimeOffset.UtcNow;
        MuleReplayClaim claim;
        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            await storage.ScheduleAsync(source.Id, Request(now), now);
            claim = Assert.Single(await storage.ClaimScheduledAsync(1, TimeSpan.FromMinutes(1), now));
        }

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            var action = Mule.Replay.MuleReplayModel.CreateAction(claim, now);
            Assert.Equal(MuleReplayMaterializationOutcome.Reconciled, await storage.MaterializeAsync(claim, action, now));
        }

        TryDelete(path);
    }

    [Theory]
    [InlineData(ConflictMode.MaterializeActionFailure)]
    [InlineData(ConflictMode.MaterializeReplayFailure)]
    [InlineData(ConflictMode.MaterializeReplayDeleted)]
    public async Task Materialize_Should_Propagate_Unrecognized_Conflicts(ConflictMode mode)
    {
        using var host = CreateHost(mode, out var path);
        var source = await SeedSourceAsync(host);
        var now = DateTimeOffset.UtcNow;
        MuleReplayClaim claim;
        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            await storage.ScheduleAsync(source.Id, Request(now), now);
            claim = Assert.Single(await storage.ClaimScheduledAsync(1, TimeSpan.FromMinutes(1), now));
        }

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            var action = Mule.Replay.MuleReplayModel.CreateAction(claim, now);
            await Assert.ThrowsAnyAsync<DbUpdateException>(() => storage.MaterializeAsync(claim, action, now));
        }

        TryDelete(path);
    }

    [Fact]
    public async Task Claim_Should_Lose_Optimistic_Concurrency_Race()
    {
        using var host = CreateHost(ConflictMode.Claim, out var path);
        var source = await SeedSourceAsync(host);
        var now = DateTimeOffset.UtcNow;
        using var scope = host.Services.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
        await storage.ScheduleAsync(source.Id, Request(now), now);

        Assert.Empty(await storage.ClaimScheduledAsync(1, TimeSpan.FromMinutes(1), now));
        TryDelete(path);
    }

    [Fact]
    public async Task Release_Should_Ignore_Optimistic_Concurrency_Race()
    {
        using var host = CreateHost(ConflictMode.Release, out var path);
        var source = await SeedSourceAsync(host);
        var now = DateTimeOffset.UtcNow;
        MuleReplayClaim claim;
        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            await storage.ScheduleAsync(source.Id, Request(now), now);
            claim = Assert.Single(await storage.ClaimScheduledAsync(1, TimeSpan.FromMinutes(1), now));
        }

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            await storage.ReleaseAsync(claim, "retry", now.AddSeconds(1));
        }

        TryDelete(path);
    }

    private static IHost CreateHost(ConflictMode mode, out string path)
    {
        path = Path.Combine(Path.GetTempPath(), $"mule-replay-conflict-{Guid.NewGuid():N}.db");
        var connectionString = $"Data Source={path}";
        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(new ReplayConflictInterceptor(connectionString, mode));
                services.AddDbContext<ConflictDbContext>((provider, options) => options
                    .UseSqlite(connectionString)
                    .AddInterceptors(provider.GetRequiredService<ReplayConflictInterceptor>()));
                services.AddMule(mule => mule.UseEntityFrameworkCore<ConflictDbContext>());
            })
            .Build();
    }

    private static async Task<DurableAction> SeedSourceAsync(IHost host)
    {
        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<ConflictDbContext>();
        await db.Database.EnsureCreatedAsync();
        var action = new DurableAction
        {
            Key = Key,
            Payload = "{}",
            PayloadType = typeof(object).AssemblyQualifiedName,
            Status = DurableActionStatus.Failed,
            CreatedOnUtc = DateTimeOffset.UtcNow,
            TerminalOnUtc = DateTimeOffset.UtcNow
        };
        db.Set<DurableAction>().Add(action);
        await db.SaveChangesAsync();
        return action;
    }

    private static MuleReplayRequest Request(DateTimeOffset now)
        => new()
        {
            RequestId = Ulid.NewUlid(),
            RequestedBy = "support",
            Reason = "conflict",
            ScheduleOnUtc = now
        };

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

    public enum ConflictMode
    {
        ScheduleRequest,
        ScheduleBranch,
        ScheduleFailure,
        Materialize,
        MaterializeActionFailure,
        MaterializeReplayFailure,
        MaterializeReplayDeleted,
        Claim,
        Release
    }

    private sealed class ConflictDbContext : DbContext
    {
        public ConflictDbContext(DbContextOptions<ConflictDbContext> options)
            : base(options)
        {
        }
    }

    private sealed class ReplayConflictInterceptor : SaveChangesInterceptor
    {
        private readonly string _connectionString;
        private readonly ConflictMode _mode;
        private int _scheduleTriggered;
        private int _actionTriggered;
        private int _replayUpdateTriggered;

        public ReplayConflictInterceptor(string connectionString, ConflictMode mode)
        {
            _connectionString = connectionString;
            _mode = mode;
        }

        public override async ValueTask<InterceptionResult<int>> SavingChangesAsync(
            DbContextEventData eventData,
            InterceptionResult<int> result,
            CancellationToken cancellationToken = default)
        {
            var context = eventData.Context;
            var addedReplay = context.ChangeTracker.Entries<MuleReplay>()
                .FirstOrDefault(x => x.State == EntityState.Added)?.Entity;
            if (addedReplay != null && Interlocked.Exchange(ref _scheduleTriggered, 1) == 0)
            {
                if (_mode == ConflictMode.ScheduleFailure)
                    throw new DbUpdateException("planned schedule failure");
                if (_mode is ConflictMode.ScheduleRequest or ConflictMode.ScheduleBranch)
                {
                    var competing = Clone(addedReplay);
                    if (_mode == ConflictMode.ScheduleBranch)
                    {
                        competing.RequestId = Ulid.NewUlid();
                        competing.ReplayActionId = Ulid.NewUlid();
                    }

                    await SaveCompetingAsync(db => db.Set<MuleReplay>().Add(competing), cancellationToken);
                }
            }

            var addedAction = context.ChangeTracker.Entries<DurableAction>()
                .FirstOrDefault(x => x.State == EntityState.Added && x.Entity.DeduplicationKey?.StartsWith("mule:replay:") == true)
                ?.Entity;
            if (_mode == ConflictMode.Materialize
                && addedAction != null
                && Interlocked.Exchange(ref _actionTriggered, 1) == 0)
            {
                await SaveCompetingAsync(db => db.Set<DurableAction>().Add(Clone(addedAction)), cancellationToken);
            }
            else if (_mode == ConflictMode.MaterializeActionFailure
                && addedAction != null
                && Interlocked.Exchange(ref _actionTriggered, 1) == 0)
            {
                throw new DbUpdateException("planned action failure");
            }

            var modifiedReplay = context.ChangeTracker.Entries<MuleReplay>()
                .FirstOrDefault(x => x.State == EntityState.Modified)?.Entity;
            if (_mode == ConflictMode.Materialize
                && modifiedReplay?.Status == MuleReplayStatus.Materialized
                && Interlocked.Exchange(ref _replayUpdateTriggered, 1) == 0)
            {
                await UpdateReplayTokenAsync(modifiedReplay.RequestId, true, cancellationToken);
            }
            else if (_mode == ConflictMode.MaterializeReplayFailure
                && modifiedReplay?.Status == MuleReplayStatus.Materialized
                && Interlocked.Exchange(ref _replayUpdateTriggered, 1) == 0)
            {
                await UpdateReplayTokenAsync(modifiedReplay.RequestId, false, cancellationToken);
            }
            else if (_mode == ConflictMode.MaterializeReplayDeleted
                && modifiedReplay?.Status == MuleReplayStatus.Materialized
                && Interlocked.Exchange(ref _replayUpdateTriggered, 1) == 0)
            {
                await DeleteReplayAsync(modifiedReplay.RequestId, cancellationToken);
            }
            else if (_mode == ConflictMode.Claim
                && modifiedReplay != null
                && modifiedReplay.LockToken != Ulid.Empty
                && Interlocked.Exchange(ref _replayUpdateTriggered, 1) == 0)
            {
                await UpdateReplayTokenAsync(modifiedReplay.RequestId, false, cancellationToken);
            }
            else if (_mode == ConflictMode.Release
                && modifiedReplay != null
                && modifiedReplay.LastError != null
                && Interlocked.Exchange(ref _replayUpdateTriggered, 1) == 0)
            {
                await UpdateReplayTokenAsync(modifiedReplay.RequestId, false, cancellationToken);
            }

            return await base.SavingChangesAsync(eventData, result, cancellationToken);
        }

        private async Task UpdateReplayTokenAsync(Ulid requestId, bool materialized, CancellationToken cancellationToken)
        {
            await SaveCompetingAsync(async db =>
            {
                var replay = await db.Set<MuleReplay>().SingleAsync(x => x.RequestId == requestId, cancellationToken);
                replay.ConcurrencyToken = Ulid.NewUlid();
                if (materialized)
                    replay.Status = MuleReplayStatus.Materialized;
            }, cancellationToken);
        }

        private async Task DeleteReplayAsync(Ulid requestId, CancellationToken cancellationToken)
        {
            await SaveCompetingAsync(async db =>
            {
                var replay = await db.Set<MuleReplay>().SingleAsync(x => x.RequestId == requestId, cancellationToken);
                db.Set<MuleReplay>().Remove(replay);
            }, cancellationToken);
        }

        private async Task SaveCompetingAsync(Action<ConflictDbContext> change, CancellationToken cancellationToken)
        {
            await using var db = CreateContext();
            change(db);
            await db.SaveChangesAsync(cancellationToken);
        }

        private async Task SaveCompetingAsync(Func<ConflictDbContext, Task> change, CancellationToken cancellationToken)
        {
            await using var db = CreateContext();
            await change(db);
            await db.SaveChangesAsync(cancellationToken);
        }

        private ConflictDbContext CreateContext()
        {
            var builder = new DbContextOptionsBuilder<ConflictDbContext>();
            builder.UseSqlite(_connectionString);
            builder.UseMuleModel();
            return new ConflictDbContext(builder.Options);
        }

        private static MuleReplay Clone(MuleReplay replay)
            => new()
            {
                RequestId = replay.RequestId,
                ConcurrencyToken = replay.ConcurrencyToken,
                SourceActionId = replay.SourceActionId,
                ReplayRootActionId = replay.ReplayRootActionId,
                ReplayActionId = replay.ReplayActionId,
                ReplaySequence = replay.ReplaySequence,
                BranchId = replay.BranchId,
                ActionKey = replay.ActionKey,
                Lane = replay.Lane,
                CorrelationId = replay.CorrelationId,
                OriginalDeduplicationKey = replay.OriginalDeduplicationKey,
                Status = replay.Status,
                RequestedBy = replay.RequestedBy,
                Reason = replay.Reason,
                Metadata = replay.Metadata,
                RequestedOnUtc = replay.RequestedOnUtc,
                ScheduleOnUtc = replay.ScheduleOnUtc
            };

        private static DurableAction Clone(DurableAction action)
            => new()
            {
                Id = action.Id,
                ConcurrencyToken = action.ConcurrencyToken,
                Key = action.Key,
                Lane = action.Lane,
                Payload = action.Payload,
                PayloadType = action.PayloadType,
                Metadata = action.Metadata,
                CorrelationId = action.CorrelationId,
                DeduplicationKey = action.DeduplicationKey,
                Status = action.Status,
                CreatedOnUtc = action.CreatedOnUtc
            };
    }
}
