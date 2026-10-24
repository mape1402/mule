namespace Mule.EntityFrameworkCore.Tests;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Mule.EntityFrameworkCore;
using Mule.Diagnostics;

public sealed class EntityFrameworkCancellationTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.cancel.ef.v1");

    [Fact]
    public async Task CancelAsync_Should_Persist_Idempotent_Cancelled_State()
    {
        await using var fixture = await CancellationFixture.CreateAsync();
        var action = CreateAction(DurableActionStatus.Pending);
        await fixture.AddAsync(action);

        MuleCancellationResult first;
        MuleCancellationResult second;
        using (var scope = fixture.Services.CreateScope())
        {
            var service = scope.ServiceProvider.GetRequiredService<IMuleCancellationService>();
            first = await service.CancelAsync(action.Id);
            second = await service.CancelAsync(action.Id);
        }

        await using var db = fixture.CreateDbContext();
        var stored = await db.Actions.AsNoTracking().SingleAsync(x => x.Id == action.Id);
        Assert.Equal(MuleCancellationOutcome.Cancelled, first.Outcome);
        Assert.Equal(MuleCancellationOutcome.AlreadyCancelled, second.Outcome);
        Assert.Equal(DurableActionStatus.Cancelled, stored.Status);
        Assert.Equal(first.CancelledOnUtc, stored.CancelledOnUtc);
        Assert.Equal(first.CancelledOnUtc, stored.TerminalOnUtc);
        Assert.Null(stored.LockedOnUtc);
        Assert.Null(stored.NextAttemptOnUtc);
    }

    [Theory]
    [InlineData(DurableActionStatus.Locked)]
    [InlineData(DurableActionStatus.Completed)]
    [InlineData(DurableActionStatus.Failed)]
    public async Task CancelAsync_Should_Reject_NonPending_Action(DurableActionStatus status)
    {
        await using var fixture = await CancellationFixture.CreateAsync();
        var action = CreateAction(status);
        await fixture.AddAsync(action);

        using var scope = fixture.Services.CreateScope();
        var result = await scope.ServiceProvider
            .GetRequiredService<IMuleCancellationService>()
            .CancelAsync(action.Id);

        Assert.Equal(MuleCancellationOutcome.TooLate, result.Outcome);
    }

    [Fact]
    public async Task CancelAsync_Should_Cancel_Scheduled_Replay_By_Reserved_Action_Id()
    {
        await using var fixture = await CancellationFixture.CreateAsync();
        var source = CreateAction(DurableActionStatus.Failed);
        await fixture.AddAsync(source);
        var request = new MuleReplayRequest
        {
            RequestId = Ulid.NewUlid(),
            ScheduleOnUtc = DateTimeOffset.UtcNow.AddHours(1)
        };
        MuleReplayResult scheduled;
        using (var scope = fixture.Services.CreateScope())
        {
            scheduled = await scope.ServiceProvider
                .GetRequiredService<IMuleReplayStorage>()
                .ScheduleAsync(source.Id, request, DateTimeOffset.UtcNow);
        }

        using (var scope = fixture.Services.CreateScope())
        {
            var service = scope.ServiceProvider.GetRequiredService<IMuleCancellationService>();
            var result = await service.CancelAsync(scheduled.ReplayActionId);
            Assert.Equal(MuleCancellationOutcome.Cancelled, result.Outcome);
            Assert.Equal(
                MuleCancellationOutcome.AlreadyCancelled,
                (await service.CancelAsync(scheduled.ReplayActionId)).Outcome);
        }

        await using var db = fixture.CreateDbContext();
        var replay = await db.Replays.AsNoTracking().SingleAsync(x => x.RequestId == request.RequestId);
        Assert.Equal(MuleReplayStatus.Cancelled, replay.Status);
        Assert.NotNull(replay.CancelledOnUtc);
    }

    [Fact]
    public async Task CancelAsync_Should_Return_NotFound_And_Reject_Claimed_Replay()
    {
        await using var fixture = await CancellationFixture.CreateAsync();
        using (var scope = fixture.Services.CreateScope())
        {
            var missing = await scope.ServiceProvider
                .GetRequiredService<IMuleCancellationService>()
                .CancelAsync(Ulid.NewUlid());
            Assert.Equal(MuleCancellationOutcome.NotFound, missing.Outcome);
        }

        var source = CreateAction(DurableActionStatus.Failed);
        await fixture.AddAsync(source);
        var now = DateTimeOffset.UtcNow;
        MuleReplayResult scheduled;
        using (var scope = fixture.Services.CreateScope())
        {
            var replays = scope.ServiceProvider.GetRequiredService<IMuleReplayStorage>();
            scheduled = await replays.ScheduleAsync(source.Id, new MuleReplayRequest
            {
                RequestId = Ulid.NewUlid(),
                ScheduleOnUtc = now
            }, now);
            Assert.Single(await replays.ClaimScheduledAsync(1, TimeSpan.FromMinutes(5), now));
        }

        using (var scope = fixture.Services.CreateScope())
        {
            var result = await scope.ServiceProvider
                .GetRequiredService<IMuleCancellationService>()
                .CancelAsync(scheduled.ReplayActionId);
            Assert.Equal(MuleCancellationOutcome.TooLate, result.Outcome);
        }
    }

    [Fact]
    public async Task CancelAsync_Should_Reject_Materialized_Replay_Status()
    {
        await using var fixture = await CancellationFixture.CreateAsync();
        var replay = CreateReplay();
        replay.Status = MuleReplayStatus.Materialized;
        await using (var db = fixture.CreateDbContext())
        {
            db.Replays.Add(replay);
            await db.SaveChangesAsync();
        }

        using var scope = fixture.Services.CreateScope();
        var result = await scope.ServiceProvider
            .GetRequiredService<IMuleCancellationService>()
            .CancelAsync(replay.ReplayActionId);

        Assert.Equal(MuleCancellationOutcome.TooLate, result.Outcome);
    }

    [Fact]
    public async Task Cancel_And_Claim_Should_Have_A_Single_Winner()
    {
        await using var fixture = await CancellationFixture.CreateAsync();
        var action = CreateAction(DurableActionStatus.Pending);
        await fixture.AddAsync(action);
        using var cancelScope = fixture.Services.CreateScope();
        using var claimScope = fixture.Services.CreateScope();
        var cancel = cancelScope.ServiceProvider.GetRequiredService<IMuleCancellationStorage>();
        var storage = claimScope.ServiceProvider.GetRequiredService<IMuleStorage>();
        var now = DateTimeOffset.UtcNow;

        var cancellationTask = cancel.CancelAsync(action.Id, now);
        var claimTask = storage.LockAsync(action.Id, TimeSpan.FromMinutes(5), now);
        await Task.WhenAll(cancellationTask, claimTask);

        var result = await cancellationTask;
        var claim = await claimTask;
        Assert.True(
            (result.IsCancelled && claim == null) ||
            (result.Outcome == MuleCancellationOutcome.TooLate && claim != null));
    }

    [Fact]
    public async Task TerminalCancellation_Should_Transition_FastLane_Owned_Lock()
    {
        await using var fixture = await CancellationFixture.CreateAsync();
        var locked = CreateAction(DurableActionStatus.Locked);
        var completed = CreateAction(DurableActionStatus.Completed);
        await fixture.AddAsync(locked, completed);
        using var scope = fixture.Services.CreateScope();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleTerminalCancellationStorage>();

        Assert.True(await storage.MarkCancelledAsync(locked.Id, DateTimeOffset.UtcNow));
        Assert.True(await storage.MarkCancelledAsync(locked.Id, DateTimeOffset.UtcNow));
        Assert.False(await storage.MarkCancelledAsync(completed.Id, DateTimeOffset.UtcNow));
        Assert.False(await storage.MarkCancelledAsync(Ulid.NewUlid(), DateTimeOffset.UtcNow));
    }

    [Fact]
    public async Task Cancellation_Should_Reconcile_Action_Concurrency_Conflicts()
    {
        await using var fixture = await ConcurrencyFixture.CreateAsync();
        var savedByCompetingReplica = CreateAction(DurableActionStatus.Pending);
        var rejectedByCompetingReplica = CreateAction(DurableActionStatus.Pending);
        await fixture.AddAsync(savedByCompetingReplica, rejectedByCompetingReplica);

        fixture.Db.ThrowAfterSave = true;
        var alreadyCancelled = await fixture.Storage.CancelAsync(savedByCompetingReplica.Id, DateTimeOffset.UtcNow);
        fixture.Db.ThrowAfterSave = false;
        fixture.Db.ThrowBeforeSave = true;
        var tooLate = await fixture.Storage.CancelAsync(rejectedByCompetingReplica.Id, DateTimeOffset.UtcNow);

        Assert.Equal(MuleCancellationOutcome.AlreadyCancelled, alreadyCancelled.Outcome);
        Assert.Equal(MuleCancellationOutcome.TooLate, tooLate.Outcome);
    }

    [Fact]
    public async Task TerminalCancellation_Should_Reconcile_Concurrency_Conflicts()
    {
        await using var fixture = await ConcurrencyFixture.CreateAsync();
        var savedByCompetingReplica = CreateAction(DurableActionStatus.Locked);
        var rejectedByCompetingReplica = CreateAction(DurableActionStatus.Locked);
        await fixture.AddAsync(savedByCompetingReplica, rejectedByCompetingReplica);

        fixture.Db.ThrowAfterSave = true;
        Assert.True(await fixture.Storage.MarkCancelledAsync(savedByCompetingReplica.Id, DateTimeOffset.UtcNow));
        fixture.Db.ThrowAfterSave = false;
        fixture.Db.ThrowBeforeSave = true;
        Assert.False(await fixture.Storage.MarkCancelledAsync(rejectedByCompetingReplica.Id, DateTimeOffset.UtcNow));
    }

    [Fact]
    public async Task Cancellation_Should_Reconcile_Replay_Concurrency_Conflicts()
    {
        await using var fixture = await ConcurrencyFixture.CreateAsync();
        var first = CreateReplay();
        var second = CreateReplay();
        await fixture.AddReplaysAsync(first, second);

        fixture.Db.ThrowAfterSave = true;
        var alreadyCancelled = await fixture.Storage.CancelAsync(first.ReplayActionId, DateTimeOffset.UtcNow);
        fixture.Db.ThrowAfterSave = false;
        fixture.Db.ThrowBeforeSave = true;
        var tooLate = await fixture.Storage.CancelAsync(second.ReplayActionId, DateTimeOffset.UtcNow);

        Assert.Equal(MuleCancellationOutcome.AlreadyCancelled, alreadyCancelled.Outcome);
        Assert.Equal(MuleCancellationOutcome.TooLate, tooLate.Outcome);
    }

    [Fact]
    public async Task Cancellation_Should_Return_NotFound_When_Action_Disappears_During_Conflict()
    {
        await using var fixture = await ConcurrencyFixture.CreateAsync();
        var action = CreateAction(DurableActionStatus.Pending);
        await fixture.AddAsync(action);
        fixture.Db.DeleteActionsBeforeThrow = true;

        var result = await fixture.Storage.CancelAsync(action.Id, DateTimeOffset.UtcNow);

        Assert.Equal(MuleCancellationOutcome.NotFound, result.Outcome);
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

    private static MuleReplay CreateReplay()
        => new()
        {
            RequestId = Ulid.NewUlid(),
            ConcurrencyToken = Ulid.NewUlid(),
            SourceActionId = Ulid.NewUlid(),
            ReplayRootActionId = Ulid.NewUlid(),
            ReplayActionId = Ulid.NewUlid(),
            BranchId = Ulid.NewUlid(),
            ActionKey = Key,
            Lane = MuleSettings.DefaultLane,
            Status = MuleReplayStatus.Scheduled,
            RequestedOnUtc = DateTimeOffset.UtcNow,
            ScheduleOnUtc = DateTimeOffset.UtcNow
        };

    private sealed class CancellationFixture : IAsyncDisposable
    {
        private readonly string _path;
        private readonly DbContextOptions<MuleDbContext> _options;

        private CancellationFixture(ServiceProvider services, string path, DbContextOptions<MuleDbContext> options)
        {
            Services = services;
            _path = path;
            _options = options;
        }

        public ServiceProvider Services { get; }

        public static async Task<CancellationFixture> CreateAsync()
        {
            var path = Path.Combine(Path.GetTempPath(), $"mule-cancel-{Guid.NewGuid():N}.db");
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddDbContext<MuleDbContext>(options => options.UseSqlite($"Data Source={path}"));
            services.AddMule(mule => mule.UseEntityFrameworkCore<MuleDbContext>());
            var provider = services.BuildServiceProvider();
            var options = provider.GetRequiredService<DbContextOptions<MuleDbContext>>();
            await using var db = new MuleDbContext(options);
            await db.Database.EnsureCreatedAsync();
            return new CancellationFixture(provider, path, options);
        }

        public MuleDbContext CreateDbContext() => new(_options);

        public async Task AddAsync(params DurableAction[] actions)
        {
            await using var db = CreateDbContext();
            db.Actions.AddRange(actions);
            await db.SaveChangesAsync();
        }

        public async ValueTask DisposeAsync()
        {
            await Services.DisposeAsync();
            try
            {
                File.Delete(_path);
            }
            catch
            {
            }
        }
    }

    private sealed class ConcurrencyFixture : IAsyncDisposable
    {
        private readonly Microsoft.Data.Sqlite.SqliteConnection _connection;

        private ConcurrencyFixture(
            Microsoft.Data.Sqlite.SqliteConnection connection,
            ConcurrencyDbContext db,
            EntityFrameworkMuleStorage<ConcurrencyDbContext> storage)
        {
            _connection = connection;
            Db = db;
            Storage = storage;
        }

        public ConcurrencyDbContext Db { get; }

        public EntityFrameworkMuleStorage<ConcurrencyDbContext> Storage { get; }

        public static async Task<ConcurrencyFixture> CreateAsync()
        {
            var connection = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
            await connection.OpenAsync();
            var options = new DbContextOptionsBuilder<ConcurrencyDbContext>()
                .UseSqlite(connection)
                .Options;
            var db = new ConcurrencyDbContext(options);
            await db.Database.EnsureCreatedAsync();
            var storage = new EntityFrameworkMuleStorage<ConcurrencyDbContext>(
                new FixedFactory<ConcurrencyDbContext>(db),
                new MuleRuntimeMetrics());
            return new ConcurrencyFixture(connection, db, storage);
        }

        public async Task AddAsync(params DurableAction[] actions)
        {
            Db.Actions.AddRange(actions);
            await Db.SaveChangesAsync();
            Db.ChangeTracker.Clear();
        }

        public async Task AddReplaysAsync(params MuleReplay[] replays)
        {
            Db.Replays.AddRange(replays);
            await Db.SaveChangesAsync();
            Db.ChangeTracker.Clear();
        }

        public async ValueTask DisposeAsync()
        {
            await Storage.DisposeAsync();
            await _connection.DisposeAsync();
        }
    }

    private sealed class ConcurrencyDbContext : DbContext
    {
        public ConcurrencyDbContext(DbContextOptions<ConcurrencyDbContext> options)
            : base(options)
        {
        }

        public DbSet<DurableAction> Actions => Set<DurableAction>();

        public DbSet<MuleReplay> Replays => Set<MuleReplay>();

        public bool ThrowBeforeSave { get; set; }

        public bool ThrowAfterSave { get; set; }

        public bool DeleteActionsBeforeThrow { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.UseMuleModel();

        public override async Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
        {
            if (DeleteActionsBeforeThrow)
            {
                await Database.ExecuteSqlRawAsync("DELETE FROM \"Actions\"", cancellationToken);
                throw new DbUpdateConcurrencyException("deleted before save");
            }

            if (ThrowBeforeSave)
                throw new DbUpdateConcurrencyException("before save");

            var result = await base.SaveChangesAsync(cancellationToken);
            if (ThrowAfterSave)
                throw new DbUpdateConcurrencyException("after save");

            return result;
        }
    }

    private sealed class FixedFactory<TDbContext> : IMuleDbContextFactory<TDbContext>
        where TDbContext : DbContext
    {
        private readonly TDbContext _dbContext;

        public FixedFactory(TDbContext dbContext)
            => _dbContext = dbContext;

        public TDbContext CreateDbContext() => _dbContext;
    }
}
