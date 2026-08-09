using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Mule;
using Mule.InMemory;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.Configure<MuleSettings>(settings =>
        {
            settings.DispatchInterval = TimeSpan.FromMilliseconds(250);
            settings.RetryDelay = TimeSpan.FromSeconds(1);
            settings.MaxAttempts = 3;
        });

        services.AddSingleton<ReceiptGateway>();
        services.AddMule(mule => mule
            .UseInMemory()
            .AddActionsFromAssemblyContaining<SendReceiptAction>());
    })
    .Build();

await host.StartAsync();

using (var scope = host.Services.CreateScope())
{
    var mule = scope.ServiceProvider.GetRequiredService<IMuleClient>();

    await mule.EnqueueAsync(
        SampleActions.SendReceipt,
        new SendReceipt("order-1001", "mario@example.com"),
        options =>
        {
            options.CorrelationId = "sample-run";
            options.Metadata["source"] = "basic-sample";
        });
}

await Task.Delay(750);

var store = host.Services.GetRequiredService<IInMemoryMule>();
var action = store.Actions.Single();

Console.WriteLine($"Action {action.Key} finished with status {action.Status}.");

await host.StopAsync();

public static class SampleActions
{
    public static readonly ActionKey SendReceipt = ActionKey.From("samples.send-receipt.v1");
}

public sealed record SendReceipt(string OrderId, string Email);

[MuleAction("samples.send-receipt.v1")]
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
    public ValueTask SendAsync(SendReceipt receipt, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Sending receipt for {receipt.OrderId} to {receipt.Email}.");
        return ValueTask.CompletedTask;
    }
}
