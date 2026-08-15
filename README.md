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

`EnqueueAsync` accepts an optional `EnqueueOptions` callback for values that belong to the durable intent but are not part of the business payload.

Use the payload for data the action needs to do the work. Use options for execution context, traceability, and idempotency:

```csharp
await mule.EnqueueAsync(
    BillingActionKeys.CapturePayment,
    new CapturePayment(orderId, amount),
    options =>
    {
        options.Lane = "payments";
        options.CorrelationId = correlationId;
        options.DeduplicationKey = orderId;
        options.Metadata["source"] = "checkout";
    },
    cancellationToken);
```

### Lane

Use `Lane` to place work in a logical partition. Lanes let slow or low-priority work run with different worker counts, batch sizes, retry settings, and parallelism than critical work.

If no lane is provided, Mule uses `default`.

The lane is stored with the durable action and is available through `MuleActionContext<TPayload>`:

```csharp
logger.LogInformation("Executing {ActionKey} on lane {Lane}.", context.Key, context.Lane);
```

### CorrelationId

Use `CorrelationId` to connect a durable action with the request, command, message, job, or workflow that produced it.

It is stored with the action and exposed to the handler through `MuleActionContext<TPayload>`:

```csharp
public ValueTask ExecuteAsync(
    MuleActionContext<CapturePayment> context,
    CancellationToken cancellationToken)
{
    logger.LogInformation(
        "Capturing payment for correlation {CorrelationId}.",
        context.CorrelationId);

    return gateway.CaptureAsync(context.Payload, cancellationToken);
}
```

Good correlation IDs are values you already use in logs or tracing, such as a request id, order id, command id, distributed trace id, or upstream message id.

### DeduplicationKey

Use `DeduplicationKey` when the same logical action could be enqueued more than once and should only have one durable intent for the same `ActionKey`.

Mule checks duplicates by the pair:

```text
ActionKey + DeduplicationKey
```

That means the same deduplication key can be reused safely by different action types, but two enqueues with the same action key and deduplication key represent the same logical work.

Common examples:

- Use an order id when sending a receipt.
- Use a payment intent id when capturing a payment.
- Use an external event id when reacting to a webhook.
- Use a command id when retrying a submitted command from the foreground.

The deduplication key should be stable and domain-owned. Avoid timestamps, random values, or generated ids when the goal is to prevent duplicate work.

Storage providers enforce deduplication at write time. When a duplicate is detected, Mule treats the enqueue as an idempotent success and keeps the existing durable action.

### Metadata

Use `Metadata` for small string values that help with diagnostics, routing, filtering, or audit.

Metadata is stored as part of the durable action and is available inside the handler:

```csharp
public ValueTask ExecuteAsync(
    MuleActionContext<CapturePayment> context,
    CancellationToken cancellationToken)
{
    var source = context.Metadata.TryGetValue("source", out var value)
        ? value
        : "unknown";

    logger.LogInformation("Capture payment source: {Source}.", source);

    return gateway.CaptureAsync(context.Payload, cancellationToken);
}
```

Keep metadata compact. If the handler needs structured business data, put that data in the payload instead.

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

Configure concurrency, retry, recovery, locking, and cleanup through `MuleSettings`:

```csharp
services.AddMule(mule => mule
    .UseInMemory()
    .Configure(settings =>
    {
        settings.ImmediateDispatch = true;
        settings.RecoveryMode = MuleRecoveryMode.Scheduled;
        settings.DispatchInterval = TimeSpan.FromSeconds(5);
        settings.DispatchBatchSize = 50;
        settings.WorkerCount = 2;
        settings.MaxDegreeOfParallelism = 8;
        settings.MaxAttempts = 10;
        settings.RetryDelay = TimeSpan.FromSeconds(30);
        settings.LockTimeout = TimeSpan.FromMinutes(5);
        settings.CleanupMode = MuleCleanupMode.Scheduled;
        settings.CleanupInterval = TimeSpan.FromMinutes(10);
        settings.CompletedRetention = TimeSpan.FromDays(1);

        settings.Lanes["payments"] = new MuleLaneSettings
        {
            WorkerCount = 2,
            MaxDegreeOfParallelism = 8,
            DispatchBatchSize = 100,
            RetryPolicy = new MuleRetryPolicy
            {
                MaxAttempts = 12,
                Delay = TimeSpan.FromSeconds(10),
                MaxDelay = TimeSpan.FromMinutes(2),
                Backoff = MuleRetryBackoff.Exponential,
                JitterRatio = 0.10
            },
            Priority = 10
        };

        settings.Lanes["notifications"] = new MuleLaneSettings
        {
            WorkerCount = 1,
            MaxDegreeOfParallelism = 2,
            DispatchBatchSize = 25,
            RetryDelay = TimeSpan.FromMinutes(1)
        };
    })
    .AddActionsFromAssemblyContaining<SendReceiptAction>());
```

`WorkerCount` controls how many workers try to claim batches.

`MaxDegreeOfParallelism` controls how many actions from the same lane can execute at the same time inside a process.

`DispatchBatchSize` controls how many eligible actions a worker claims in one storage operation.

`DispatchQueueCapacity` controls the in-memory immediate dispatch queue capacity. Use `0` for an unbounded queue.

`LockTimeout` is the lease duration. If a process dies while an action is locked, the action becomes claimable again after the lock expires.

Lane settings override the global worker count, batch size, parallelism, retry delay, max attempts, and priority for actions enqueued into that lane.

`RetryPolicy` can be configured globally or per lane. It supports fixed, linear, and exponential backoff, optional max delay, and optional jitter:

```csharp
settings.RetryPolicy = new MuleRetryPolicy
{
    MaxAttempts = 10,
    Delay = TimeSpan.FromSeconds(15),
    MaxDelay = TimeSpan.FromMinutes(5),
    Backoff = MuleRetryBackoff.Exponential,
    JitterRatio = 0.15
};
```

The older `MaxAttempts` and `RetryDelay` settings remain supported. Mule uses them when no `RetryPolicy` is configured.

Lanes with higher `Priority` are claimed before lower-priority lanes during recovery cycles. Lanes with the same priority are processed together.

`RecoveryMode` controls how pending work is recovered:

- `Polling`: checks storage every `DispatchInterval`.
- `Scheduled`: checks storage on startup, then wakes when an action is due for retry or when immediate dispatch is disabled and new work is persisted.

`CleanupMode` controls completed action retention:

- `Disabled`: completed actions are kept for audit/history.
- `Polling`: cleanup runs every `CleanupInterval`.
- `Scheduled`: cleanup wakes when completed actions reach `CompletedRetention`.

Mule uses atomic storage claims and locks so multiple service replicas can run workers at the same time without intentionally executing the same locked action concurrently. Actions should still be idempotent because Mule provides at-least-once execution.

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
- expired lock count
- duplicates ignored by idempotency
- throughput per minute
- runtime completed count
- runtime failed count
- oldest pending timestamp
- oldest locked timestamp
- oldest failed timestamp
- oldest pending age
- oldest locked age
- average enqueue-to-execution latency
- average execution latency
- average enqueue-to-terminal latency
- backlog by lane
- backlog by action key
- failures by action key
- retries by action key

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
