namespace Mule.EntityFrameworkCore.Tests;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Mule.Diagnostics;
using Mule.EntityFrameworkCore;
using Mule.EntityFrameworkCore.Internal;
using System.Reflection;

#pragma warning disable EF1001

public sealed class EntityFrameworkStorageCoverageTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.ef.coverage");

    [Fact]
    public async Task Storage_Should_Handle_Deduplication_Claims_Terminals_Cleanup_And_Diagnostics()
    {
        using var host = CreateHost(out var databasePath);
        await EnsureDatabaseAsync(host);
        var now = DateTimeOffset.UtcNow;
        var existing = CreateAction("existing", now.AddMinutes(-5), deduplicationKey: "dedupe");
        var duplicateId = CreateAction("duplicate-id", now);
        var retry = CreateAction("retry", now.AddMinutes(-3));
        var future = CreateAction("future", now.AddMinutes(-2), nextAttemptOnUtc: now.AddMinutes(5));
        var dueWithDate = CreateAction("due-with-date", now.AddMinutes(-2), nextAttemptOnUtc: now.AddSeconds(-1));
        var expired = CreateAction("expired", now.AddMinutes(-1), DurableActionStatus.Locked, lockedOnUtc: now.AddMinutes(-10));
        var active = CreateAction("active", now, DurableActionStatus.Locked, lockedOnUtc: now);
        active.Lane = " ";
        var lockedWithoutDate = CreateAction("locked-without-date", now, DurableActionStatus.Locked);
        var completed = CreateAction("completed", now.AddMinutes(-20), DurableActionStatus.Completed, completedOnUtc: now.AddMinutes(-10));
        var completedRecent = CreateAction("completed-recent", now, DurableActionStatus.Completed, completedOnUtc: now.AddMinutes(1));

        duplicateId.Id = existing.Id;

        using (var scope = host.Services.CreateScope())
        {
            var storage = (IMuleStorage)scope.ServiceProvider.GetRequiredService<IMuleStorage>();

            await storage.AddRangeAsync([]);
            await Assert.ThrowsAsync<ArgumentNullException>(() => storage.AddAsync(null));
            await Assert.ThrowsAsync<ArgumentNullException>(() => storage.AddRangeAsync(null));
            await Assert.ThrowsAsync<ArgumentException>(() => storage.AddRangeAsync([null]));

            await storage.AddRangeAsync(
            [
                existing,
                duplicateId,
                CreateAction("dedupe-again", now, deduplicationKey: "dedupe"),
                retry,
                future,
                dueWithDate,
                expired,
                active,
                lockedWithoutDate,
                completed,
                completedRecent
            ]);
            await storage.SaveChangesAsync();

            Assert.Null(await storage.FindByDeduplicationKeyAsync(Key, ""));
            Assert.Equal(existing.Id, await storage.FindByDeduplicationKeyAsync(Key, "dedupe"));
            Assert.Null(await storage.LockAsync(Ulid.NewUlid(), TimeSpan.FromMinutes(1), now));
            Assert.Null(await storage.LockAsync(active.Id, TimeSpan.FromMinutes(5), now));
            Assert.Null(await storage.LockAsync(future.Id, TimeSpan.FromMinutes(5), now));
            Assert.Null(await storage.LockAsync(completed.Id, TimeSpan.FromMinutes(5), now));

            var next = await storage.GetNextPendingOnUtcAsync(TimeSpan.FromMinutes(1), now);
            Assert.NotNull(next);

            var claimed = await storage.ClaimPendingAsync("Lane-A", 10, TimeSpan.FromMinutes(1), now);
            Assert.Contains(claimed, x => x.Id == existing.Id);
            Assert.Contains(claimed, x => x.Id == retry.Id);
            Assert.Contains(claimed, x => x.Id == expired.Id);
            Assert.Contains(claimed, x => x.Id == dueWithDate.Id);
            Assert.DoesNotContain(claimed, x => x.Id == future.Id);

            var retryClaim = claimed.Single(x => x.Id == retry.Id);
            await storage.MarkFailedAsync(retryClaim.Id, "retry later", now, now.AddSeconds(5));
            var terminalClaim = claimed.Single(x => x.Id == expired.Id);
            await storage.MarkFailedAsync(terminalClaim.Id, "terminal", now, null);
            await ((IMuleBatchTerminalStorage)storage).MarkCompletedRangeAsync([]);
            await Assert.ThrowsAsync<ArgumentNullException>(() => ((IMuleBatchTerminalStorage)storage).MarkCompletedRangeAsync(null));
            await ((IMuleBatchTerminalStorage)storage).MarkCompletedRangeAsync(
            [
                new MuleCompletedAction(existing.Id, now.AddSeconds(10))
            ]);
            await Assert.ThrowsAsync<InvalidOperationException>(() => storage.MarkCompletedAsync(Ulid.NewUlid(), now));

            Assert.Equal(completed.CompletedOnUtc, await storage.GetOldestCompletedOnUtcAsync());
            Assert.Equal(0, await storage.CleanCompletedAsync(now.AddMinutes(-30), 10));
            Assert.Equal(1, await storage.CleanCompletedAsync(now.AddMinutes(-1), 1));
            await storage.SaveChangesAsync();
        }

        using (var scope = host.Services.CreateScope())
        {
            var diagnostics = scope.ServiceProvider.GetRequiredService<IMuleDiagnostics>();
            var snapshot = await diagnostics.GetSnapshotAsync();

            Assert.True(snapshot.Completed >= 1);
            Assert.True(snapshot.Failed >= 1);
            Assert.True(snapshot.Pending >= 1);
            Assert.NotNull(snapshot.OldestPendingOnUtc);
            Assert.NotNull(snapshot.OldestFailedOnUtc);
            Assert.True(snapshot.BacklogByLane.ContainsKey("Lane-A"));
            Assert.True(snapshot.FailuresByActionKey.ContainsKey(Key));
            Assert.True(snapshot.RetriesByActionKey.ContainsKey(Key));
            Assert.NotNull(snapshot.AverageEnqueueToExecutionLatency);
            Assert.NotNull(snapshot.AverageEnqueueToTerminalLatency);
        }

        TryDelete(databasePath);
    }

    [Fact]
    public async Task SaveChanges_Should_Record_Duplicate_When_Host_Configures_Unique_Dedupe_Index()
    {
        using var host = CreateHost(
            out var databasePath,
            options => options.ConfigureActions(entity =>
            {
                entity.HasIndex(x => new { x.Key, x.DeduplicationKey }).IsUnique();
            }));
        await EnsureDatabaseAsync(host);
        var first = CreateAction("first", DateTimeOffset.UtcNow, deduplicationKey: "unique");
        var second = CreateAction("second", DateTimeOffset.UtcNow, deduplicationKey: "unique");

        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        db.Set<DurableAction>().Add(first);
        await db.SaveChangesAsync();
        var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
        await storage.AddAsync(second);
        await storage.SaveChangesAsync();

        var metrics = scope.ServiceProvider.GetRequiredService<MuleRuntimeMetrics>().GetSnapshot(DateTimeOffset.UtcNow);
        Assert.Equal(1, metrics.DuplicatesIgnored);
        Assert.Equal(1, await db.Set<DurableAction>().CountAsync());

        TryDelete(databasePath);
    }

    [Fact]
    public async Task SaveChanges_Should_Record_Duplicate_When_Host_Unique_Index_Fails()
    {
        var options = new DbContextOptionsBuilder<ThrowingDbContext>()
            .UseSqlite("Data Source=:memory:")
            .Options;
        var db = new ThrowingDbContext(options);
        db.Set<DurableAction>().Add(CreateAction("duplicate", DateTimeOffset.UtcNow));
        var metrics = new MuleRuntimeMetrics();
        var storage = new EntityFrameworkMuleStorage<ThrowingDbContext>(new FixedFactory<ThrowingDbContext>(db), metrics);

        await storage.SaveChangesAsync();

        Assert.Equal(1, metrics.GetSnapshot(DateTimeOffset.UtcNow).DuplicatesIgnored);
    }

    [Fact]
    public async Task Storage_Should_Cover_Existing_Ids_Default_Lane_And_Concurrency_Conflicts()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptionsBuilder<ConcurrencyDbContext>()
            .UseSqlite(connection)
            .Options;
        var db = new ConcurrencyDbContext(options);
        await db.Database.EnsureCreatedAsync();
        var now = DateTimeOffset.UtcNow;
        var action = CreateAction("concurrency", now.AddSeconds(-1));
        action.DeduplicationKey = null;
        action.Lane = " ";
        db.Set<DurableAction>().Add(action);
        await db.SaveChangesAsync();
        db.ChangeTracker.Clear();
        var metrics = new MuleRuntimeMetrics();
        var storage = new EntityFrameworkMuleStorage<ConcurrencyDbContext>(new FixedFactory<ConcurrencyDbContext>(db), metrics);

        await storage.AddAsync(action);
        await storage.AddRangeAsync([action]);
        Assert.Empty(await storage.ClaimPendingAsync("other", 1, TimeSpan.FromMinutes(1), now));

        db.ThrowConcurrency = true;
        Assert.Null(await storage.LockAsync(action.Id, TimeSpan.FromMinutes(1), now));
        Assert.Empty(db.ChangeTracker.Entries<DurableAction>().Where(x => x.State != EntityState.Unchanged));

        await storage.DisposeAsync();
    }

    [Fact]
    public async Task Diagnostics_Should_Return_Empty_Snapshot_With_Positive_Lock_Timeout()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlite(connection);
        optionsBuilder.UseMuleModel();
        var options = optionsBuilder.Options;
        var factory = new InlineFactory(options);
        await using var diagnostics = new EntityFrameworkMuleDiagnostics<TestDbContext>(
            factory,
            Options.Create(new MuleSettings { LockTimeout = TimeSpan.FromSeconds(1) }),
            new MuleRuntimeMetrics());
        await using (var db = factory.CreateDbContext())
            await db.Database.EnsureCreatedAsync();

        var snapshot = await diagnostics.GetSnapshotAsync();

        Assert.Equal(0, snapshot.Pending);
        Assert.Null(snapshot.OldestPendingAge);
        Assert.Null(snapshot.OldestLockedAge);
        Assert.Null(snapshot.AverageEnqueueToExecutionLatency);
        Assert.Null(snapshot.AverageExecutionLatency);
        Assert.Null(snapshot.AverageEnqueueToTerminalLatency);

        await using var defaultDiagnostics = new EntityFrameworkMuleDiagnostics<TestDbContext>(
            factory,
            Options.Create(new MuleSettings { LockTimeout = TimeSpan.Zero }),
            new MuleRuntimeMetrics());
        Assert.Equal(0, (await defaultDiagnostics.GetSnapshotAsync()).Pending);
    }

    [Fact]
    public void Storage_Helpers_Should_Classify_Provider_And_Unique_Constraint_Failures()
    {
        Assert.True(InvokeStorageHelper("ShouldFallbackToClientFiltering", new InvalidOperationException()));
        Assert.True(InvokeStorageHelper("ShouldFallbackToClientFiltering", new NotSupportedException()));
        Assert.False(InvokeStorageHelper("ShouldFallbackToClientFiltering", new Exception()));

        Assert.True(InvokeStorageHelper("IsUniqueConstraintViolation", new DbUpdateException("x", new Exception("unique"))));
        Assert.True(InvokeStorageHelper("IsUniqueConstraintViolation", new DbUpdateException("x", new Exception("duplicate"))));
        Assert.False(InvokeStorageHelper("IsUniqueConstraintViolation", new DbUpdateException("x", new Exception("other"))));
        Assert.False(InvokeStorageHelper("IsUniqueConstraintViolation", new DbUpdateException("x")));
        Assert.Equal(MuleSettings.DefaultLane, InvokeStorageHelper<string>("NormalizeLane", " "));
        Assert.Equal("lane", InvokeStorageHelper<string>("NormalizeLane", "lane"));
    }

    [Fact]
    public void ServiceRegistration_And_OptionsExtension_Should_Cover_Null_And_Resolution_Paths()
    {
        Assert.Throws<ArgumentNullException>(() => EntityFrameworkMuleServiceCollectionExtensions.UseEntityFrameworkMule(null, _ => { }));
        Assert.Throws<ArgumentNullException>(() => new ServiceCollection().UseEntityFrameworkMule(null));
        Assert.Throws<ArgumentNullException>(() => EntityFrameworkMuleServiceCollectionExtensions.UseEntityFrameworkCore<TestDbContext>(null));

        var services = new ServiceCollection();
        services.AddOptions<MuleSettings>();
        services.AddSingleton<MuleRuntimeMetrics>();
        Assert.Throws<InvalidOperationException>(() =>
            services.AddMule(mule => mule.UseEntityFrameworkCore<TestDbContext>()));

        var extension = new MuleDbContextOptionsExtension(options => options.ConfigureActions(_ => { }));
        var info = extension.Info;
        var debug = new Dictionary<string, string>();
        info.PopulateDebugInfo(debug);

        Assert.False(info.IsDatabaseProvider);
        Assert.Equal("using Mule ", info.LogFragment);
        Assert.Equal("1", debug["Mule"]);
        Assert.True(info.ShouldUseSameServiceProvider(new MuleDbContextOptionsExtension().Info));
        Assert.NotEqual(0, info.GetServiceProviderHashCode());

        var applyServices = new ServiceCollection();
        extension.ApplyServices(applyServices);
        extension.Validate(null);
        Assert.Contains(applyServices, descriptor => descriptor.ServiceType == typeof(IModelCustomizer));

        var instanceServices = new ServiceCollection();
        var instanceOptions = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlite("Data Source=:memory:")
            .Options;
        instanceServices.AddSingleton(instanceOptions);
        instanceServices.AddOptions<MuleSettings>();
        instanceServices.AddSingleton<MuleRuntimeMetrics>();
        instanceServices.AddMule(mule => mule.UseEntityFrameworkCore<TestDbContext>());
        using var provider = instanceServices.BuildServiceProvider();
        Assert.NotNull(provider.GetRequiredService<DbContextOptions<TestDbContext>>());

        var resolveOptions = typeof(EntityFrameworkMuleServiceCollectionExtensions).GetMethod(
            "ResolveOptions",
            BindingFlags.Static | BindingFlags.NonPublic).MakeGenericMethod(typeof(TestDbContext));
        var implementationTypeDescriptor = ServiceDescriptor.Transient(
            typeof(DbContextOptions<TestDbContext>),
            typeof(DbContextOptions<TestDbContext>));
        var resolvedByType = resolveOptions.Invoke(null, [implementationTypeDescriptor, provider]);
        Assert.IsType<DbContextOptions<TestDbContext>>(resolvedByType);
    }

    [Fact]
    public void Constructors_Should_Validate_Dependencies()
    {
        var metrics = new MuleRuntimeMetrics();
        var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlite("Data Source=:memory:");
        optionsBuilder.UseMuleModel();
        var factory = new InlineFactory(optionsBuilder.Options);

        Assert.Throws<ArgumentNullException>(() => new EntityFrameworkMuleStorage<TestDbContext>(null, metrics));
        Assert.Throws<ArgumentNullException>(() => new EntityFrameworkMuleStorage<TestDbContext>(factory, null));
        Assert.Throws<ArgumentNullException>(() => new EntityFrameworkMuleDiagnostics<TestDbContext>(null, Options.Create(new MuleSettings()), metrics));
        Assert.Throws<ArgumentNullException>(() => new EntityFrameworkMuleDiagnostics<TestDbContext>(factory, null, metrics));
        Assert.Throws<ArgumentNullException>(() => new EntityFrameworkMuleDiagnostics<TestDbContext>(factory, Options.Create(new MuleSettings()), null));
        var muleOptionsBuilder = new DbContextOptionsBuilder<MuleDbContext>()
            .UseSqlite("Data Source=:memory:");
        muleOptionsBuilder.UseMuleModel();
        var provider = new ServiceCollection().BuildServiceProvider();
        using var storage = new EntityFrameworkMuleStorage(new MuleDbContextFactory<MuleDbContext>(provider, muleOptionsBuilder.Options), metrics);
        var diagnosticsOptionsBuilder = new DbContextOptionsBuilder<MuleDbContext>()
            .UseSqlite("Data Source=:memory:");
        diagnosticsOptionsBuilder.UseMuleModel();
        using var diagnostics = new EntityFrameworkMuleDiagnostics(
            new MuleDbContextFactory<MuleDbContext>(provider, diagnosticsOptionsBuilder.Options),
            Options.Create(new MuleSettings()),
            metrics);
        Assert.Throws<ArgumentNullException>(() => ((ModelBuilder)null).UseMuleModel());
        Assert.Throws<ArgumentNullException>(() => ((DbContextOptionsBuilder<TestDbContext>)null).UseMuleModel());
        Assert.Throws<ArgumentNullException>(() => new MuleEntityFrameworkOptions().ConfigureActions(null));
        Assert.Throws<ArgumentNullException>(() => new MuleDbContextFactory<TestDbContext>(null, optionsBuilder.Options));
        Assert.Throws<ArgumentNullException>(() => new MuleDbContextFactory<TestDbContext>(provider, null));
    }

    private static bool InvokeStorageHelper(string method, Exception exception)
        => InvokeStorageHelper<bool>(method, exception);

    private static T InvokeStorageHelper<T>(string method, object argument)
    {
        var methodInfo = typeof(EntityFrameworkMuleStorage<TestDbContext>).GetMethod(
            method,
            BindingFlags.Static | BindingFlags.NonPublic);
        return (T)methodInfo.Invoke(null, [argument]);
    }

    private static IHost CreateHost(
        out string databasePath,
        Action<MuleEntityFrameworkOptions> configureMule = null)
    {
        databasePath = Path.Combine(Path.GetTempPath(), $"mule-ef-coverage-{Guid.NewGuid():N}.db");
        var capturedPath = databasePath;

        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddDbContext<TestDbContext>(options => options.UseSqlite($"Data Source={capturedPath}"));
                services.AddMule(mule => mule.UseEntityFrameworkCore<TestDbContext>(configureMule));
            })
            .Build();
    }

    private static async Task EnsureDatabaseAsync(IHost host)
    {
        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        await db.Database.EnsureCreatedAsync();
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
            NextAttemptOnUtc = nextAttemptOnUtc,
            Attempts = status == DurableActionStatus.Failed ? 1 : 0
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

    private sealed class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions<TestDbContext> options)
            : base(options)
        {
        }
    }

    private sealed class InlineFactory : IMuleDbContextFactory<TestDbContext>
    {
        private readonly DbContextOptions<TestDbContext> _options;

        public InlineFactory(DbContextOptions<TestDbContext> options)
        {
            _options = options;
        }

        public TestDbContext CreateDbContext()
            => new(_options);
    }

    private sealed class FixedFactory<TDbContext> : IMuleDbContextFactory<TDbContext>
        where TDbContext : DbContext
    {
        private readonly TDbContext _dbContext;

        public FixedFactory(TDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public TDbContext CreateDbContext()
            => _dbContext;
    }

    private sealed class ThrowingDbContext : DbContext
    {
        public ThrowingDbContext(DbContextOptions<ThrowingDbContext> options)
            : base(options)
        {
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.UseMuleModel();

        public override Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
            => throw new DbUpdateException("duplicate", new InvalidOperationException("unique"));
    }

    private sealed class ConcurrencyDbContext : DbContext
    {
        public ConcurrencyDbContext(DbContextOptions<ConcurrencyDbContext> options)
            : base(options)
        {
        }

        public bool ThrowConcurrency { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.UseMuleModel();

        public override Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
            => ThrowConcurrency
                ? throw new DbUpdateConcurrencyException("concurrency")
                : base.SaveChangesAsync(cancellationToken);
    }
}

#pragma warning restore EF1001
