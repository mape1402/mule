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
                    mule.For<CaptureTestPayloadAction, TestPayload>(Key);
                });
                services.UseEntityFrameworkMule(options => options.UseSqlite($"Data Source={capturedPath}"));
            })
            .Build();
    }

    private static async Task EnsureDatabaseAsync(IHost host)
    {
        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
        await db.Database.EnsureCreatedAsync();
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
}
