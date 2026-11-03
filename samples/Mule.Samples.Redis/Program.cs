using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule;
using Mule.Diagnostics;
using Mule.Redis;

var connectionString = Environment.GetEnvironmentVariable("MULE_REDIS_CONNECTION_STRING")
    ?? "localhost:6379";
var runId = Ulid.NewUlid().ToString();

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddSingleton<PaymentGateway>();
        services.AddMule(mule => mule
            .UseRedis(options =>
            {
                options.ConnectionString = connectionString;
                options.KeyPrefix = "mule-redis-sample";
            })
            .Configure(settings =>
            {
                settings.DispatchInterval = TimeSpan.FromMilliseconds(100);
                settings.CleanupMode = MuleCleanupMode.Disabled;
            })
            .AddActionsFromAssemblyContaining<CapturePaymentAction>());
    })
    .Build();

await host.StartAsync();

Ulid actionId;
using (var scope = host.Services.CreateScope())
{
    var mule = scope.ServiceProvider.GetRequiredService<IMuleClient>();
    actionId = await mule.EnqueueAsync(
        SampleActions.CapturePayment,
        new CapturePayment($"order-{runId}", 149.90m),
        options =>
        {
            options.CorrelationId = $"checkout-{runId}";
            options.DeduplicationKey = $"payment-{runId}";
            options.Metadata["source"] = "redis-sample";
        });
}

await host.Services.GetRequiredService<PaymentGateway>().WaitAsync(TimeSpan.FromSeconds(10));

using (var scope = host.Services.CreateScope())
{
    var diagnostics = scope.ServiceProvider.GetRequiredService<IMuleDiagnostics>();
    var deadline = DateTimeOffset.UtcNow.AddSeconds(10);
    MuleDiagnosticsSnapshot snapshot;
    do
    {
        snapshot = await diagnostics.GetSnapshotAsync();
        if (snapshot.Completed > 0)
            break;
        await Task.Delay(50);
    }
    while (DateTimeOffset.UtcNow < deadline);

    Console.WriteLine($"Action {actionId} completed. Redis reports {snapshot.Completed} completed action(s).");
}

await host.StopAsync();

public static class SampleActions
{
    public static readonly ActionKey CapturePayment = ActionKey.From("samples.capture-payment.v1");
}

public sealed record CapturePayment(string OrderId, decimal Amount);

[MuleAction("samples.capture-payment.v1")]
public sealed class CapturePaymentAction : IMuleAction<CapturePayment>
{
    private readonly PaymentGateway _gateway;

    public CapturePaymentAction(PaymentGateway gateway)
    {
        _gateway = gateway;
    }

    public ValueTask ExecuteAsync(
        MuleActionContext<CapturePayment> context,
        CancellationToken cancellationToken)
        => _gateway.CaptureAsync(context.Payload, cancellationToken);
}

public sealed class PaymentGateway
{
    private readonly TaskCompletionSource _completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ValueTask CaptureAsync(CapturePayment payment, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Captured {payment.Amount:C} for {payment.OrderId}.");
        _completed.TrySetResult();
        return ValueTask.CompletedTask;
    }

    public Task WaitAsync(TimeSpan timeout)
        => _completed.Task.WaitAsync(timeout);
}
