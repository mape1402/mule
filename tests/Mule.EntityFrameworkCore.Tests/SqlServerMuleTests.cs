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
