# Mule

Mule is a .NET library for durable, resilient action execution.

Application code records an intent in the foreground. Mule persists that intent, then a background dispatcher executes the registered action with locking, retries, recovery, cleanup, and diagnostics.

Mule provides at-least-once execution. Actions should be idempotent or use deduplication keys.

## Packages

- `Mule.DurableActions`: core API, dispatcher, registration, serialization, and contracts.
- `Mule.DurableActions.InMemory`: in-memory provider for tests, samples, and local experiments.
- `Mule.DurableActions.EntityFrameworkCore`: EF Core provider for durable storage.

Namespaces intentionally stay short:

```csharp
using Mule;
using Mule.InMemory;
using Mule.EntityFrameworkCore;
```

## Action Keys

Mule persists action identity as a string, but the public API uses `ActionKey` so string values are deliberate and easy to centralize.

```csharp
public static class BillingActions
{
    public static readonly ActionKey CapturePayment =
        ActionKey.From("billing.capture-payment.v1");
}
```

There is no implicit conversion from `string` to `ActionKey`. Treat key changes as durable schema changes.

## Discover Actions

The recommended pattern is to put the durable key on the action class and let Mule discover actions from an assembly. You do not register every action in DI, and you do not list every action in startup.

```csharp
[MuleAction("billing.capture-payment.v1")]
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
```

```csharp
services.AddSingleton<PaymentGateway>();

services.AddMule(mule =>
{
    mule.AddActionsFromAssemblyContaining<CapturePaymentAction>();
});
```

Mule discovers classes marked with `[MuleAction(...)]`, verifies they implement exactly one `IMuleAction<TPayload>`, and creates them with `ActivatorUtilities` when they execute. Only their real dependencies belong in DI.

The payload type is not the durable identity. This keeps generic payloads and envelopes like `Payload<T>` from accidentally changing action identity.

Manual registration is still available for advanced cases, but it should not be the default for large applications:

```csharp
mule.For<CapturePaymentAction, CapturePayment>(
    BillingActions.CapturePayment);
```

If you already have an application service registered in DI and want Mule to call it directly, the service-backed overload is also available:

```csharp
mule.For<PaymentGateway, CapturePayment>(
    BillingActions.CapturePayment,
    static (gateway, context, cancellationToken) =>
        gateway.CaptureAsync(context.Payload, cancellationToken));
```

## Choose Storage

For tests or local usage:

```csharp
services.UseInMemoryMule();
```

For EF Core:

```csharp
services.UseEntityFrameworkMule(options =>
    options.UseSqlServer(connectionString));
```

For SQLite or local samples:

```csharp
services.UseEntityFrameworkMule(options =>
    options.UseSqlite("Data Source=mule.db"));
```

Create the schema with migrations or call `EnsureCreated()` for simple apps:

```csharp
using var scope = app.Services.CreateScope();
var db = scope.ServiceProvider.GetRequiredService<MuleDbContext>();
await db.Database.EnsureCreatedAsync();
```

## Enqueue Work

Foreground code records the intent and returns quickly:

```csharp
await mule.EnqueueAsync(
    BillingActions.CapturePayment,
    new CapturePayment(orderId, amount),
    options =>
    {
        options.CorrelationId = correlationId;
        options.DeduplicationKey = orderId;
        options.Metadata["source"] = "checkout";
    },
    cancellationToken);
```

`EnqueueAsync` does not require the handler to be present in the producer process. That allows producer-only services to record intents while worker processes execute them.

## Run The Sample

```bash
dotnet run --project samples/Mule.Samples.Basic/Mule.Samples.Basic.csproj --framework net8.0
```

Expected output:

```text
Sending receipt for order-1001 to mario@example.com.
Action samples.send-receipt.v1 finished with status Completed.
```

## Diagnostics

Providers can expose `IMuleDiagnostics`:

```csharp
var diagnostics = scope.ServiceProvider.GetRequiredService<IMuleDiagnostics>();
var snapshot = await diagnostics.GetSnapshotAsync();
```

The snapshot includes counts for pending, locked, completed, and failed actions, plus oldest pending and failed timestamps.
