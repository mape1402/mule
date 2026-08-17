namespace Mule.EntityFrameworkCore.Tests;

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
        var actionId = Guid.NewGuid();

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
        await probe.WaitAsync();
        await WaitForActionStatusAsync(host, DurableActionStatus.Completed);
        await host.StopAsync();

        Assert.Equal("hello", Assert.Single(probe.Values));

        using var verificationScope = host.Services.CreateScope();
        var db = verificationScope.ServiceProvider.GetRequiredService<MuleDbContext>();
        var action = await db.Actions.AsNoTracking().SingleAsync();
        Assert.Equal(DurableActionStatus.Completed, action.Status);

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

    private static IHost CreateAppDbContextHost(out string databasePath)
    {
        databasePath = Path.Combine(Path.GetTempPath(), $"mule-app-{Guid.NewGuid():N}.db");
        var capturedPath = databasePath;

        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddDbContext<TestDbContext>(options => options.UseSqlite($"Data Source={capturedPath}"));
                services.AddMule(mule => mule
                    .UseEntityFrameworkCore<TestDbContext>()
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
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));

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
            _probe.Values.Add(context.Payload.Value);
            _probe.Signal();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestProbe
    {
        private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public List<string> Values { get; } = new();

        public void Signal()
            => _completion.TrySetResult();

        public async Task WaitAsync()
            => await _completion.Task.WaitAsync(TimeSpan.FromSeconds(3));
    }

    private sealed class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions<TestDbContext> options)
            : base(options)
        {
        }
    }
}
