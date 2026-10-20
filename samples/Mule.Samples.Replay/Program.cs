using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule;
using Mule.InMemory;

var capturePayment = ActionKey.From("billing.capture-payment.v1");
using var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services => services.AddMule(mule => mule
        .Configure(settings =>
        {
            settings.MaxAttempts = 1;
            settings.Replay.RecoveryMode = MuleRecoveryMode.Scheduled;
        })
        .UseInMemory()
        .For<CapturePaymentAction, CapturePayment>(capturePayment)))
    .Build();

await host.StartAsync();

Ulid failedActionId;
using (var scope = host.Services.CreateScope())
{
    var mule = scope.ServiceProvider.GetRequiredService<IMuleClient>();
    failedActionId = await mule.EnqueueAsync(
        capturePayment,
        new CapturePayment("invoice-42", 125.50m),
        options =>
        {
            options.CorrelationId = "checkout-42";
            options.Metadata["tenant"] = "north";
        });
}

await WaitForActionAsync(host, failedActionId, DurableActionStatus.Failed);
CapturePaymentAction.DependencyAvailable = true;

MuleReplayResult result;
using (var scope = host.Services.CreateScope())
{
    var replays = scope.ServiceProvider.GetRequiredService<IMuleReplayService>();
    result = await replays.ReplayAsync(failedActionId, new MuleReplayRequest
    {
        RequestId = Ulid.NewUlid(),
        RequestedBy = "support@example.test",
        Reason = "Payment dependency recovered",
        Metadata = new Dictionary<string, string>
        {
            ["ticket"] = "INC-1042"
        }
    });
}

var completed = await WaitForActionAsync(host, result.ReplayActionId, DurableActionStatus.Completed);
Console.WriteLine($"Original: {failedActionId} (Failed)");
Console.WriteLine($"Replay:   {completed.Id} ({completed.Status})");

await host.StopAsync();

static async Task<DurableAction> WaitForActionAsync(IHost host, Ulid id, DurableActionStatus status)
{
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    while (!timeout.IsCancellationRequested)
    {
        var action = host.Services
            .GetRequiredService<IInMemoryMule>()
            .Actions
            .SingleOrDefault(x => x.Id == id);
        if (action?.Status == status)
            return action;

        await Task.Delay(25, timeout.Token);
    }

    throw new TimeoutException($"Action '{id}' did not reach '{status}'.");
}

internal sealed record CapturePayment(string InvoiceId, decimal Amount);

internal sealed class CapturePaymentAction : IMuleAction<CapturePayment>
{
    public static bool DependencyAvailable { get; set; }

    public ValueTask ExecuteAsync(MuleActionContext<CapturePayment> context, CancellationToken cancellationToken)
    {
        if (!DependencyAvailable)
            throw new InvalidOperationException("The payment dependency is unavailable.");

        Console.WriteLine($"Captured {context.Payload.Amount:C} for {context.Payload.InvoiceId}.");
        return ValueTask.CompletedTask;
    }
}
