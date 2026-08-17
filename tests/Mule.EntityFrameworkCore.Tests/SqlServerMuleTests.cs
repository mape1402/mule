namespace Mule.EntityFrameworkCore.Tests;

using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule.EntityFrameworkCore;

public sealed class SqlServerMuleTests
{
    public const string ConnectionStringEnvironmentVariable = "MULE_SQLSERVER_CONNECTION_STRING";

    private static readonly ActionKey Key = ActionKey.From("tests.sql.capture.v1");

    [SqlServerFact]
    public async Task EnqueueAsync_Should_Return_Existing_Id_For_Concurrent_Duplicate()
    {
        using var database = SqlServerTestDatabase.Create();
        using var host = CreateHost(database.ConnectionString);
        await EnsureDatabaseAsync(host);

        var ids = await Task.WhenAll(Enumerable.Range(0, 25).Select(async _ =>
        {
            using var scope = host.Services.CreateScope();
            var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
            return await client.EnqueueAsync(
                    Key,
                    new TestPayload("duplicate"),
                    options => options.DeduplicationKey = "same-work")
                .AsTask();
        }));

        using var verificationScope = host.Services.CreateScope();
        var db = verificationScope.ServiceProvider.GetRequiredService<MuleDbContext>();

        Assert.Single(ids.Distinct());
        Assert.Equal(ids[0], await db.Actions.Select(x => x.Id).SingleAsync());
    }

    [SqlServerFact]
    public async Task ClaimPendingAsync_Should_Claim_Batches_Atomically_Across_Replicas()
    {
        using var database = SqlServerTestDatabase.Create();
        using var host = CreateHost(database.ConnectionString);
        await EnsureDatabaseAsync(host);

        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
            db.Actions.AddRange(Enumerable.Range(0, 100).Select(index => new DurableAction
            {
                Key = Key,
                Payload = "{}",
                PayloadType = typeof(TestPayload).AssemblyQualifiedName,
                Status = DurableActionStatus.Pending,
                CreatedOnUtc = DateTimeOffset.UtcNow.AddTicks(index)
            }));
            await db.SaveChangesAsync();
        }

        var ids = new List<Guid>();
        for (var wave = 0; wave < 10 && ids.Count < 100; wave++)
        {
            var claims = await Task.WhenAll(Enumerable.Range(0, 10).Select(async _ =>
            {
                using var scope = host.Services.CreateScope();
                var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
                return await storage.ClaimPendingAsync(
                    MuleSettings.DefaultLane,
                    10,
                    TimeSpan.FromMinutes(5),
                    DateTimeOffset.UtcNow);
            }));

            ids.AddRange(claims.SelectMany(x => x).Select(x => x.Id));
        }

        Assert.Equal(100, ids.Count);
        Assert.Equal(100, ids.Distinct().Count());
    }

    [SqlServerFact]
    public async Task EnqueueManyAsync_Should_Persist_Batch()
    {
        using var database = SqlServerTestDatabase.Create();
        using var host = CreateHost(database.ConnectionString);
        await EnsureDatabaseAsync(host);

        using (var scope = host.Services.CreateScope())
        {
            var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
            var ids = await client.EnqueueManyAsync(Enumerable.Range(0, 100)
                .Select(index => MuleIntent.For(Key, new TestPayload($"batch-{index}"))));

            Assert.Equal(100, ids.Count);
            Assert.Equal(100, ids.Distinct().Count());
        }

        using var verificationScope = host.Services.CreateScope();
        var db = verificationScope.ServiceProvider.GetRequiredService<MuleDbContext>();

        Assert.Equal(100, await db.Actions.CountAsync());
    }

    [SqlServerFact]
    public async Task MarkCompletedAsync_Should_Update_Action_With_Direct_Sql()
    {
        using var database = SqlServerTestDatabase.Create();
        using var host = CreateHost(database.ConnectionString);
        await EnsureDatabaseAsync(host);
        var actionId = await InsertActionAsync(host, DurableActionStatus.Locked);
        var completedOnUtc = DateTimeOffset.UtcNow;

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
            await storage.MarkCompletedAsync(actionId, completedOnUtc);
            await storage.SaveChangesAsync();
        }

        using var verificationScope = host.Services.CreateScope();
        var db = verificationScope.ServiceProvider.GetRequiredService<MuleDbContext>();
        var action = await db.Actions.AsNoTracking().SingleAsync();

        Assert.Equal(DurableActionStatus.Completed, action.Status);
        Assert.NotNull(action.CompletedOnUtc);
        Assert.Null(action.LockedOnUtc);
    }

    [SqlServerFact]
    public async Task MarkFailedAsync_Should_Update_Action_With_Direct_Sql()
    {
        using var database = SqlServerTestDatabase.Create();
        using var host = CreateHost(database.ConnectionString);
        await EnsureDatabaseAsync(host);
        var actionId = await InsertActionAsync(host, DurableActionStatus.Locked);

        using (var scope = host.Services.CreateScope())
        {
            var storage = scope.ServiceProvider.GetRequiredService<IMuleStorage>();
            await storage.MarkFailedAsync(actionId, "planned", DateTimeOffset.UtcNow, null);
            await storage.SaveChangesAsync();
        }

        using var verificationScope = host.Services.CreateScope();
        var db = verificationScope.ServiceProvider.GetRequiredService<MuleDbContext>();
        var action = await db.Actions.AsNoTracking().SingleAsync();

        Assert.Equal(DurableActionStatus.Failed, action.Status);
        Assert.Equal(1, action.Attempts);
        Assert.Equal("planned", action.LastError);
        Assert.Null(action.LockedOnUtc);
        Assert.NotNull(action.TerminalOnUtc);
    }

    [SqlServerFact]
    public async Task CleanCompletedAsync_Should_Delete_Completed_Actions_In_Batch()
    {
        using var database = SqlServerTestDatabase.Create();
        using var host = CreateHost(database.ConnectionString);
        await EnsureDatabaseAsync(host);

        using (var scope = host.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
            db.Actions.AddRange(Enumerable.Range(0, 5).Select(index => new DurableAction
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

            Assert.Equal(2, deleted);
        }

        using var verificationScope = host.Services.CreateScope();
        var verificationDb = verificationScope.ServiceProvider.GetRequiredService<MuleDbContext>();

        Assert.Equal(3, await verificationDb.Actions.CountAsync());
    }

    private static IHost CreateHost(string connectionString)
        => Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddMule();
                services.UseEntityFrameworkMule(options => options.UseSqlServer(connectionString));
            })
            .Build();

    private static async Task EnsureDatabaseAsync(IHost host)
    {
        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
        await db.Database.EnsureCreatedAsync();
    }

    private static async Task<Guid> InsertActionAsync(IHost host, DurableActionStatus status)
    {
        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
        var action = new DurableAction
        {
            Key = Key,
            Payload = "{}",
            PayloadType = typeof(TestPayload).AssemblyQualifiedName,
            Status = status,
            LockedOnUtc = status == DurableActionStatus.Locked ? DateTimeOffset.UtcNow : null,
            CreatedOnUtc = DateTimeOffset.UtcNow
        };
        db.Actions.Add(action);
        await db.SaveChangesAsync();
        return action.Id;
    }

    private sealed record TestPayload(string Value);

    private sealed class SqlServerTestDatabase : IDisposable
    {
        private SqlServerTestDatabase(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public string ConnectionString { get; }

        public static SqlServerTestDatabase Create()
        {
            var builder = new SqlConnectionStringBuilder(Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable))
            {
                InitialCatalog = $"MuleSqlTests_{Guid.NewGuid():N}"
            };

            return new SqlServerTestDatabase(builder.ConnectionString);
        }

        public void Dispose()
        {
            var builder = new SqlConnectionStringBuilder(ConnectionString);
            var databaseName = builder.InitialCatalog;
            builder.InitialCatalog = "master";

            using var connection = new SqlConnection(builder.ConnectionString);
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = $"""
IF DB_ID(N'{databaseName}') IS NOT NULL
BEGIN
    ALTER DATABASE [{databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [{databaseName}];
END
""";
            command.ExecuteNonQuery();
        }
    }
}
