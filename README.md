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

## Register Actions

Register Mule and map an `ActionKey` to a service method:

```csharp
services.AddSingleton<PaymentService>();

services.AddMule(mule =>
{
    mule.For<PaymentService, CapturePayment>(
        BillingActions.CapturePayment,
        static (service, context, cancellationToken) =>
            service.CaptureAsync(context.Payload, cancellationToken));
});
```

The payload type is not the durable identity. This keeps generic payloads and envelopes like `Payload<T>` from accidentally changing action identity.

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
