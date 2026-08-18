# Mule

Mule is a .NET library for durable, resilient action execution.

Your application records an intent in the foreground. Mule stores that intent, then a background dispatcher executes the matching action with locking, retries, recovery, cleanup, and diagnostics.

Mule provides **at-least-once execution**. Actions should be idempotent, or you should use deduplication keys for operations that cannot safely run twice.

## Packages

```bash
dotnet add package Mule.DurableActions
dotnet add package Mule.DurableActions.InMemory
dotnet add package Mule.DurableActions.EntityFrameworkCore
dotnet add package Mule.DurableActions.FastLane.InMemory
dotnet add package Mule.DurableActions.FastLane.Redis
dotnet add package Mule.DurableActions.Testing
```

- `Mule.DurableActions`: core API, dispatcher, registration, serialization, and contracts.
- `Mule.DurableActions.InMemory`: in-memory provider for tests, samples, and local experiments.
- `Mule.DurableActions.EntityFrameworkCore`: EF Core provider for durable storage.
- `Mule.DurableActions.FastLane.InMemory`: optional in-memory buffer that accepts foreground work quickly and flushes durable storage in batches.
- `Mule.DurableActions.FastLane.Redis`: optional Redis-backed buffer for high-throughput, multi-replica foreground work.
- `Mule.DurableActions.Testing`: test harness helpers for durable action assertions.

Package IDs are descriptive, but namespaces stay short:

```csharp
using Mule;
using Mule.Configuration;
using Mule.InMemory;
using Mule.EntityFrameworkCore;
using Mule.FastLane.InMemory;
using Mule.FastLane.Redis;
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

### Batch Enqueue

Use `EnqueueManyAsync` when a foreground workflow needs to register many durable intents at once:

```csharp
var ids = await mule.EnqueueManyAsync([
    MuleIntent.For(
        BillingActionKeys.CapturePayment,
        new CapturePayment("order-1001", 120.00m),
        options =>
        {
            options.Lane = "payments";
            options.DeduplicationKey = "payment-intent-1001";
        }),
    MuleIntent.For(
        ReceiptActionKeys.SendReceipt,
        new SendReceipt("order-1001", "mario@example.com"),
        options => options.CorrelationId = "checkout-1001")
], cancellationToken);
```

`EnqueueManyAsync` stores the intents in one storage save operation where the provider supports it, then notifies Mule to dispatch each accepted action. Each `MuleIntent` carries its own `ActionKey`, payload, lane, correlation id, deduplication key, and metadata.

The returned ids match the input order. If an intent uses a deduplication key and the work already exists, Mule returns the existing durable action id for that item.

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

When the duplicate already exists, `EnqueueAsync` returns the existing action id instead of the transient id from the ignored enqueue attempt. This lets callers safely correlate follow-up work with the durable row that Mule will execute.

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

For existing SQL Server installations upgrading to the lane and latency model, use [docs/sql-server-upgrade-1.2.0.sql](/C:/elysium/mule/docs/sql-server-upgrade-1.2.0.sql) as the reference migration. It adds lane and runtime timestamp columns, creates the filtered deduplication constraint, and adds the lane/status/due-date index used by the dispatcher.

### FastLane InMemory

FastLane InMemory is an optional buffer for high-throughput producers:

```csharp
services.AddMule(mule => mule
    .UseEntityFrameworkCore<AppDbContext>()
    .UseFastLaneInMemory(options =>
    {
        options.IntentFlushSize = 500;
        options.CompletionFlushSize = 1_000;
        options.FlushInterval = TimeSpan.FromMilliseconds(50);
    })
    .AddActionsFromAssemblyContaining<SendReceiptAction>());
```

With FastLane InMemory, `EnqueueAsync` and `EnqueueManyAsync` acknowledge after the intent is accepted by the in-process buffer. Mule can execute that buffered work immediately, while a background flusher persists intents to the durable provider in batches. Terminal states are also buffered and flushed after their intents have been persisted, so durable storage observes the intent before `Completed` or `Failed`.

This mode is faster than direct EF Core writes, but it is intentionally less durable. If the process exits before a flush, buffered intents or terminal updates that have not reached the durable provider can be lost. Use it when the producer can tolerate that window.

### FastLane Redis

FastLane Redis is an optional shared buffer for high-throughput services running with multiple replicas:

```csharp
services.AddMule(mule => mule
    .UseEntityFrameworkCore<AppDbContext>()
    .UseFastLaneRedis(options =>
    {
        options.ConnectionString = redisConnectionString;
        options.KeyPrefix = "checkout";
        options.IntentFlushSize = 1_000;
        options.CompletionFlushSize = 2_000;
        options.FlushInterval = TimeSpan.FromMilliseconds(25);
        options.LeaseDuration = TimeSpan.FromMinutes(2);
        options.DeduplicationRetention = TimeSpan.FromDays(7);
    })
    .AddActionsFromAssemblyContaining<SendReceiptAction>());
```

With FastLane Redis, foreground enqueue writes to Redis first and returns without waiting for an EF Core insert. Mule workers can claim buffered work from Redis immediately, using per-action Redis leases so multiple replicas do not execute the same buffered intent at the same time. A background flusher persists intents to the durable provider in batches, then flushes terminal states only after the intent exists in durable storage.

Redis does not replace EF Core durable storage. Treat it as a fast shared front buffer in front of the durable provider. If Redis is configured with volatile persistence or data is evicted before Mule flushes it, unflushed intents can be lost. Use Redis persistence and memory policies that match the durability window your workload can tolerate.

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
        settings.MaxDrainBatchesPerCycle = 4;
        settings.MaxDrainActionsPerCycle = 1_000;
        settings.DrainUntilEmpty = false;
        settings.YieldBetweenDrainBatches = TimeSpan.FromMilliseconds(1);
        settings.ExecutionQueueCapacity = 10_000;
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
            DispatchQueueCapacity = 1_000,
            ExecutionQueueCapacity = 5_000,
            PollingInterval = TimeSpan.FromSeconds(2),
            MaxDrainBatchesPerCycle = 8,
            MaxDrainActionsPerCycle = 2_000,
            DrainUntilEmpty = true,
            YieldBetweenDrainBatches = TimeSpan.FromMilliseconds(1),
            RetryPolicy = new MuleRetryPolicy
            {
                MaxAttempts = 12,
                Delay = TimeSpan.FromSeconds(10),
                MaxDelay = TimeSpan.FromMinutes(2),
                Backoff = MuleRetryBackoff.Exponential,
                JitterRatio = 0.10
            },
            Priority = 10,
            Weight = 5
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

`WorkerCount` controls how many readers/claimers admit work for a lane.

`MaxDegreeOfParallelism` controls how many already-claimed actions from the same lane can execute at the same time inside a process.

Readers and claimers do not execute actions inline. They lock or claim durable actions, enqueue them into a bounded lane executor, and immediately continue admitting more work unless the lane executor applies backpressure.

`DispatchBatchSize` controls how many eligible actions a worker claims in one storage operation. Recovery workers claim pending work in batches and dispatch those batches to the lane executor instead of claiming and executing one action at a time.

`MaxDrainBatchesPerCycle` controls how many claim batches a worker can drain from a lane before yielding to the next recovery cycle.

`MaxDrainActionsPerCycle` caps the total actions claimed from a lane in one drain cycle. Use `0` for no action cap.

`DrainUntilEmpty` lets recovery continue claiming from a lane until storage returns no due work or another drain cap is reached.

`YieldBetweenDrainBatches` inserts a small delay between drain batches so high-throughput recovery does not monopolize CPU or storage connections.

`DispatchQueueCapacity` controls the in-memory immediate dispatch queue capacity. Use `0` for an unbounded queue.

Lane-level `DispatchQueueCapacity` lets a busy lane absorb foreground enqueue bursts without letting that lane consume the full process queue. If omitted or set to `0`, the lane uses the global queue capacity.

`ExecutionQueueCapacity` controls the per-lane queue of locked or claimed actions waiting for an execution slot. Use a bounded value for high-volume workloads so memory cannot grow without limit. When the execution queue is saturated, Mule readers wait before taking more durable locks whenever possible.

Lane-level `PollingInterval` controls how often that lane checks durable storage when `RecoveryMode` is `Polling`. If omitted or set to `TimeSpan.Zero`, the lane uses the global `DispatchInterval`.

`LockTimeout` is the lease duration. If a process dies while an action is locked, the action becomes claimable again after the lock expires.

Lane settings override the global worker count, batch size, drain limits, queue capacity, polling interval, parallelism, retry delay, max attempts, priority, and scheduling weight for actions enqueued into that lane.

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

`Priority` and `Weight` guide lane scheduling. Higher-priority lanes get more opportunities, but Mule uses weighted fair scheduling so lower-priority lanes still receive turns while high-priority lanes have constant backlog.

Immediate dispatch starts workers for each configured lane using that lane's `WorkerCount`. Mule also keeps fallback workers for lanes that are used without explicit lane configuration, so simple applications can keep using the `default` lane only.

`ConfigureHighThroughputRuntime()` applies productive generic defaults for durable inbox/outbox-style workloads. `ConfigureLane(...)` is a chainable helper for lane-specific tuning:

```csharp
services.AddMule(mule => mule
    .UseEntityFrameworkCore<AppDbContext>()
    .ConfigureHighThroughputRuntime()
    .ConfigureLane("responses", lane =>
    {
        lane.Priority = 100;
        lane.Weight = 10;
        lane.WorkerCount = 16;
        lane.MaxDegreeOfParallelism = 128;
        lane.DispatchBatchSize = 500;
        lane.DispatchQueueCapacity = 10_000;
        lane.ExecutionQueueCapacity = 20_000;
        lane.DrainUntilEmpty = true;
    })
    .ConfigureLane("dispatches", lane =>
    {
        lane.Priority = 50;
        lane.Weight = 5;
        lane.WorkerCount = 8;
        lane.MaxDegreeOfParallelism = 64;
    })
    .AddActionsFromAssemblyContaining<SendReceiptAction>());
```

`RecoveryMode` controls how pending work is recovered:

- `Polling`: checks storage every `DispatchInterval`.
- `Scheduled`: checks storage on startup, then wakes when an action is due for retry or when immediate dispatch is disabled and new work is persisted.

Use `Polling` when another process may insert work without notifying the current process, or when you prefer a simple heartbeat. Use `Scheduled` when Mule owns the enqueue path in the current application and you want to avoid storage polling while there is no pending or failed work.

`CleanupMode` controls completed action retention:

- `Disabled`: completed actions are kept for audit/history.
- `Polling`: cleanup runs every `CleanupInterval`.
- `Scheduled`: cleanup wakes when completed actions reach `CompletedRetention`.

Cleanup is batch-oriented. `CleanupBatchSize` controls how many completed actions can be removed per storage operation. SQL Server uses direct batch delete statements for completed actions instead of loading entities one by one.

Mule uses atomic storage claims and locks so multiple service replicas can run workers at the same time without intentionally executing the same locked action concurrently. Actions should still be idempotent because Mule provides at-least-once execution.

For large workloads, tune `WorkerCount` and `MaxDegreeOfParallelism` independently. Increase `WorkerCount` when storage admission is slow. Increase `MaxDegreeOfParallelism` when handlers are the bottleneck. Use `ExecutionQueueCapacity` to bound claimed work waiting behind slow handlers.

SQL Server claims use update locks with `READPAST`, so a replica may skip rows that another replica is locking at that exact moment. That is expected: later waves or later polling cycles can claim the remaining rows without duplicating work.

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
- completed per minute by lane
- completed per minute by action key
- runtime completed count
- runtime failed count
- runtime failed count by lane
- runtime failed count by action key
- claimed count by lane
- waiting execution count by lane
- executing count by lane
- executor saturation count by lane
- oldest waiting execution age by lane
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

The EF Core test suite includes optional SQL Server integration tests. Set `MULE_SQLSERVER_CONNECTION_STRING` to run them locally; without that variable, they are skipped. The tests create and drop temporary databases and validate concurrent deduplication plus atomic claiming across replica-like workers.

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
