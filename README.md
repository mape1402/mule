# Mule

Mule is a .NET library for durable, resilient action execution.

Your application records an intent in the foreground. Mule stores that intent, then a background dispatcher executes the matching action with locking, retries, recovery, cleanup, and diagnostics.

Mule provides **at-least-once execution**. Actions should be idempotent, or you should use deduplication keys for operations that cannot safely run twice.

## Packages

```bash
dotnet add package Mule.DurableActions
dotnet add package Mule.DurableActions.InMemory
dotnet add package Mule.DurableActions.EntityFrameworkCore
dotnet add package Mule.DurableActions.Testing
```

- `Mule.DurableActions`: core API, dispatcher, registration, serialization, and contracts.
- `Mule.DurableActions.InMemory`: in-memory provider for tests, samples, and local experiments.
- `Mule.DurableActions.EntityFrameworkCore`: EF Core provider for durable storage.
- `Mule.DurableActions.Testing`: test harness helpers for durable action assertions.

Package IDs are descriptive, but namespaces stay short:

```csharp
using Mule;
using Mule.InMemory;
using Mule.EntityFrameworkCore;
using Mule.Testing;
```

## Concepts

- **Action key**: a durable, stable identifier for a kind of work.
- **Payload**: the serialized data Mule stores with the intent.
- **Action**: a class that implements `IMuleAction<TPayload>`.
- **Client**: `IMuleClient`, used by foreground code to enqueue work.
- **Storage provider**: InMemory or EF Core.
- **Dispatcher**: a hosted background service that locks and executes pending actions.

Mule stores the action key as a string, but public APIs use `ActionKey` to avoid loose string usage.

## Quick Start

Define a payload:

```csharp
public sealed record SendReceipt(string OrderId, string Email);
```

Define an action. The `[MuleAction]` attribute is the durable key.

```csharp
[MuleAction("receipts.send.v1")]
public sealed class SendReceiptAction : IMuleAction<SendReceipt>
{
    private readonly ReceiptGateway _gateway;

    public SendReceiptAction(ReceiptGateway gateway)
    {
        _gateway = gateway;
    }

    public ValueTask ExecuteAsync(
        MuleActionContext<SendReceipt> context,
        CancellationToken cancellationToken)
    {
        return _gateway.SendAsync(context.Payload, cancellationToken);
    }
}
```

Register Mule once and let it discover actions from an assembly:

```csharp
services.AddSingleton<ReceiptGateway>();

services.AddMule(mule => mule
    .UseInMemory()
    .AddActionsFromAssemblyContaining<SendReceiptAction>());
```

Enqueue work from foreground code:

```csharp
var mule = scope.ServiceProvider.GetRequiredService<IMuleClient>();

await mule.EnqueueAsync(
    ActionKey.From("receipts.send.v1"),
    new SendReceipt("order-1001", "mario@example.com"),
    cancellationToken);
```

Mule persists the intent, queues it for background execution, and retries it if the action fails.

## Action Discovery

The recommended registration style is assembly discovery:

```csharp
services.AddMule(mule => mule
    .AddActionsFromAssemblyContaining<SendReceiptAction>());
```

Mule scans the assembly for concrete classes marked with `[MuleAction(...)]`.

Each discovered action must implement exactly one `IMuleAction<TPayload>`:

```csharp
[MuleAction("billing.capture-payment.v1")]
public sealed class CapturePaymentAction : IMuleAction<CapturePayment>
{
    public ValueTask ExecuteAsync(
        MuleActionContext<CapturePayment> context,
        CancellationToken cancellationToken)
    {
        // Execute durable work here.
        return ValueTask.CompletedTask;
    }
}
```

Actions are **not** registered in DI one by one. Mule creates the action with `ActivatorUtilities` when it executes, so constructor dependencies are still resolved from the application service provider.

## Action Keys

Keys should be stable and versioned:

```csharp
public static class BillingActionKeys
{
    public static readonly ActionKey CapturePayment =
        ActionKey.From("billing.capture-payment.v1");
}
```

Use the same value in the attribute:

```csharp
[MuleAction("billing.capture-payment.v1")]
public sealed class CapturePaymentAction : IMuleAction<CapturePayment>
{
    // ...
}
```

There is no implicit conversion from `string` to `ActionKey`. Treat key changes as durable schema changes.

The payload type is not the durable identity. That keeps generic payloads and envelopes such as `Payload<T>` from accidentally changing action identity.

## Enqueue Options

Use `EnqueueOptions` for correlation, metadata, and deduplication:

```csharp
await mule.EnqueueAsync(
    BillingActionKeys.CapturePayment,
    new CapturePayment(orderId, amount),
    options =>
    {
        options.CorrelationId = correlationId;
        options.DeduplicationKey = orderId;
        options.Metadata["source"] = "checkout";
    },
    cancellationToken);
```

`EnqueueAsync` does not require the handler to be present in the producer process. Producer-only services can record intents while worker services discover and execute the actions.

## Storage Providers

### InMemory

Use InMemory for tests and samples:

```csharp
services.AddMule(mule => mule
    .UseInMemory()
    .AddActionsFromAssemblyContaining<SendReceiptAction>());
```

The provider is process-local and non-durable. It exposes `IInMemoryMule` for assertions:

```csharp
var store = provider.GetRequiredService<IInMemoryMule>();
var action = Assert.Single(store.Actions);
```

### Entity Framework Core

Use EF Core for durable storage:

```csharp
services.AddDbContext<AppDbContext>(options =>
    options.UseSqlServer(connectionString));

services.AddMule(mule => mule
    .UseEntityFrameworkCore<AppDbContext>()
    .AddActionsFromAssemblyContaining<SendReceiptAction>());
```

For SQLite:

```csharp
services.AddDbContext<AppDbContext>(options =>
    options.UseSqlite("Data Source=mule.db"));
```

Mule adds its table to the application `DbContext` model automatically. Create the schema with migrations, or call `EnsureCreated()` in simple apps:

```csharp
using var scope = app.Services.CreateScope();
var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
await db.Database.EnsureCreatedAsync();
```

`UseEntityFrameworkMule(...)` and `MuleDbContext` remain available for standalone storage, but `UseEntityFrameworkCore<TDbContext>()` is the recommended integration for applications that already have a DbContext.

## Dispatcher Settings

Configure retry, recovery, locking, and cleanup through `MuleSettings`:

```csharp
services.AddMule(mule => mule
    .UseInMemory()
    .Configure(settings =>
    {
        settings.ImmediateDispatch = true;
        settings.RecoveryMode = MuleRecoveryMode.Scheduled;
        settings.DispatchInterval = TimeSpan.FromSeconds(5);
        settings.DispatchBatchSize = 50;
        settings.MaxAttempts = 10;
        settings.RetryDelay = TimeSpan.FromSeconds(30);
        settings.LockTimeout = TimeSpan.FromMinutes(5);
        settings.CleanupMode = MuleCleanupMode.Scheduled;
        settings.CleanupInterval = TimeSpan.FromMinutes(10);
        settings.CompletedRetention = TimeSpan.FromDays(1);
    })
    .AddActionsFromAssemblyContaining<SendReceiptAction>());
```

`RecoveryMode` controls how pending work is recovered:

- `Polling`: checks storage every `DispatchInterval`.
- `Scheduled`: checks storage on startup, then wakes when an action is due for retry or when immediate dispatch is disabled and new work is persisted.

`CleanupMode` controls completed action retention:

- `Disabled`: completed actions are kept for audit/history.
- `Polling`: cleanup runs every `CleanupInterval`.
- `Scheduled`: cleanup wakes when completed actions reach `CompletedRetention`.

Mule uses storage locks so multiple service replicas can run workers at the same time without intentionally executing the same locked action concurrently. Actions should still be idempotent because Mule provides at-least-once execution.

## Diagnostics

Providers expose `IMuleDiagnostics`:

```csharp
var diagnostics = scope.ServiceProvider.GetRequiredService<IMuleDiagnostics>();
var snapshot = await diagnostics.GetSnapshotAsync();
```

The snapshot includes:

- pending count
- locked count
- completed count
- failed count
- oldest pending timestamp
- oldest failed timestamp

## Testing

Use the testing package to register Mule with in-memory storage and a test harness:

```csharp
services.AddMule(mule => mule
    .UseTesting()
    .AddActionsFromAssemblyContaining<SendReceiptAction>());
```

Then wait for actions deterministically in tests:

```csharp
var actionId = await mule.EnqueueAsync(
    BillingActionKeys.CapturePayment,
    new CapturePayment(orderId, amount),
    cancellationToken);

var harness = provider.GetRequiredService<IMuleTestHarness>();
var action = await harness.WaitForActionAsync(
    actionId,
    DurableActionStatus.Completed);
```

## Manual Registration

Assembly discovery is the default recommendation. Manual registration remains available for advanced cases:

```csharp
mule.For<CapturePaymentAction, CapturePayment>(
    BillingActionKeys.CapturePayment);
```

If you already have an application service registered in DI and want Mule to call it directly:

```csharp
mule.For<PaymentGateway, CapturePayment>(
    BillingActionKeys.CapturePayment,
    static (gateway, context, cancellationToken) =>
        gateway.CaptureAsync(context.Payload, cancellationToken));
```

Use these overloads sparingly in large applications. Discovery keeps startup composition clean as action count grows.

## Samples

Run the basic sample:

```bash
dotnet run --project samples/Mule.Samples.Basic/Mule.Samples.Basic.csproj --framework net8.0
```

Expected output:

```text
Sending receipt for order-1001 to mario@example.com.
Action samples.send-receipt.v1 finished with status Completed.
```

Run the testing sample:

```bash
dotnet run --project samples/Mule.Samples.Testing/Mule.Samples.Testing.csproj --framework net8.0
```

Expected output:

```text
Observed action samples.testing.send-receipt.v1 with status Completed.
Captured receipt for order-1001 to mario@example.com.
```
