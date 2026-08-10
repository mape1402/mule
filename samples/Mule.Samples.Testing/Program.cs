using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule;
using Mule.Testing;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddSingleton<ReceiptGateway>();
        services.AddSingleton<ReceiptProbe>();

        services.AddMule(mule => mule
            .UseTesting()
            .AddActionsFromAssemblyContaining<SendReceiptAction>());
    })
    .Build();

await host.StartAsync();

Guid actionId;

using (var scope = host.Services.CreateScope())
{
    var mule = scope.ServiceProvider.GetRequiredService<IMuleClient>();

    actionId = await mule.EnqueueAsync(
        SampleActions.SendReceipt,
        new SendReceipt("order-1001", "mario@example.com"),
        options =>
        {
            options.CorrelationId = "testing-sample";
            options.Metadata["source"] = "testing-sample";
        });
}

var harness = host.Services.GetRequiredService<IMuleTestHarness>();
var action = await harness.WaitForActionAsync(
    actionId,
    DurableActionStatus.Completed);

var receipt = host.Services.GetRequiredService<ReceiptProbe>().SentReceipts.Single();

Console.WriteLine($"Observed action {action.Key} with status {action.Status}.");
Console.WriteLine($"Captured receipt for {receipt.OrderId} to {receipt.Email}.");

await host.StopAsync();

public static class SampleActions
{
    public static readonly ActionKey SendReceipt = ActionKey.From("samples.testing.send-receipt.v1");
}

public sealed record SendReceipt(string OrderId, string Email);

[MuleAction("samples.testing.send-receipt.v1")]
public sealed class SendReceiptAction : IMuleAction<SendReceipt>
{
    private readonly ReceiptGateway _gateway;

    public SendReceiptAction(ReceiptGateway gateway)
    {
        _gateway = gateway;
    }

    public ValueTask ExecuteAsync(MuleActionContext<SendReceipt> context, CancellationToken cancellationToken)
        => _gateway.SendAsync(context.Payload, cancellationToken);
}

public sealed class ReceiptGateway
{
    private readonly ReceiptProbe _probe;

    public ReceiptGateway(ReceiptProbe probe)
    {
        _probe = probe;
    }

    public ValueTask SendAsync(SendReceipt receipt, CancellationToken cancellationToken)
    {
        _probe.SentReceipts.Add(receipt);
        return ValueTask.CompletedTask;
    }
}

public sealed class ReceiptProbe
{
    public List<SendReceipt> SentReceipts { get; } = new();
}
