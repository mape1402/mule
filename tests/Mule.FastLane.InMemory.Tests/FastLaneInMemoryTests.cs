namespace Mule.FastLane.InMemory.Tests;

using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule.EntityFrameworkCore;

[Collection("FastLane InMemory")]
public sealed class FastLaneInMemoryTests
{
    private static readonly ActionKey Key = ActionKey.From("tests.fastlane.capture.v1");

    [Fact]
    public async Task EnqueueManyAsync_Should_Buffer_Batch_And_Flush_Completions_After_Intents()
    {
        using var host = CreateHost(out var databasePath);
        await EnsureDatabaseAsync(host);
        await host.StartAsync();

        using (var scope = host.Services.CreateScope())
        {
            var client = scope.ServiceProvider.GetRequiredService<IMuleClient>();
            await client.EnqueueManyAsync(Enumerable.Range(0, 25)
                .Select(index => MuleIntent.For(Key, new TestPayload($"batch-{index}"))));
        }

        var probe = host.Services.GetRequiredService<TestProbe>();
        await probe.WaitForCountAsync(25);
        await WaitForDurableCountAsync(host, DurableActionStatus.Completed, 25);
        await host.StopAsync();

        Assert.Equal(25, probe.Values.Count);

        TryDelete(databasePath);
    }

    private static IHost CreateHost(out string databasePath)
    {
        databasePath = Path.Combine(Path.GetTempPath(), $"mule-fastlane-{Guid.NewGuid():N}.db");
        var capturedPath = databasePath;

        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.Configure<MuleSettings>(settings =>
                {
                    settings.DispatchInterval = TimeSpan.FromMilliseconds(25);
                    settings.RecoveryMode = MuleRecoveryMode.Scheduled;
                    settings.RetryDelay = TimeSpan.FromMilliseconds(25);
                    settings.MaxAttempts = 1;
                    settings.CleanupMode = MuleCleanupMode.Disabled;
                });

                services.AddSingleton<TestProbe>();
                services.AddDbContext<MuleDbContext>(options => options.UseSqlite($"Data Source={capturedPath}"));
                services.AddMule(mule => mule
                    .UseEntityFrameworkCore<MuleDbContext>()
                    .UseFastLaneInMemory(options =>
                    {
                        options.IntentFlushSize = 100;
                        options.CompletionFlushSize = 100;
                        options.FlushInterval = TimeSpan.FromMilliseconds(10);
                    })
                    .AddActionsFromAssemblyContaining<FastLaneInMemoryTests>());
            })
            .Build();
    }

    private static async Task EnsureDatabaseAsync(IHost host)
    {
        using var scope = host.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
        await db.Database.EnsureCreatedAsync();
    }

    private static async Task WaitForDurableCountAsync(IHost host, DurableActionStatus status, int count)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!timeout.IsCancellationRequested)
        {
            using var scope = host.Services.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
            if (await db.Actions.AsNoTracking().CountAsync(x => x.Status == status, timeout.Token) >= count)
                return;

            await Task.Delay(25, timeout.Token);
        }
    }

    private static void TryDelete(string databasePath)
    {
        try
        {
            if (File.Exists(databasePath))
                File.Delete(databasePath);
        }
        catch
        {
        }
    }

    private sealed record TestPayload(string Value);

    [MuleAction("tests.fastlane.capture.v1")]
    private sealed class TestAction : IMuleAction<TestPayload>
    {
        private readonly TestProbe _probe;

        public TestAction(TestProbe probe)
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

        public ConcurrentBag<string> Values { get; } = new();

        public void Signal()
            => _completion.TrySetResult();

        public async Task WaitForCountAsync(int count)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            while (!timeout.IsCancellationRequested)
            {
                if (Values.Count >= count)
                    return;

                await Task.Delay(25, timeout.Token);
            }
        }
    }
}

[CollectionDefinition("FastLane InMemory", DisableParallelization = true)]
public sealed class FastLaneInMemoryCollection
{
}
