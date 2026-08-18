# Changelog

All notable changes to Mule packages will be documented in this file.

## [Unreleased]

## [v1.4.0] - 2026-08-18

### Added

- Added a bounded lane action executor that separates reading/claiming from action execution.
- Added `ExecutionQueueCapacity` globally and per lane for claimed actions waiting on execution slots.
- Added executor diagnostics for claimed actions, waiting execution count, executing count, saturation, and oldest waiting execution age by lane.
- Added integration coverage for non-blocking queue readers, real `MaxDegreeOfParallelism` enforcement, recovery through the executor, and bounded executor backpressure.
- Added `MuleIntent` and `IMuleClient.EnqueueManyAsync(...)` for registering multiple durable intents with a single client call.
- Added batch storage insertion support for Mule storage providers.
- Added SQL Server integration coverage for batch enqueue, direct completion updates, and batch cleanup deletes.
- Added `Mule.DurableActions.FastLane.InMemory`, an optional in-process buffer that acknowledges enqueue after buffer write and flushes intents and terminal states to durable storage in batches.
- Added `Mule.DurableActions.FastLane.Redis`, an optional Redis-backed FastLane buffer for multi-replica high-throughput workloads.
- Added `IMuleDurableStorage` so optional buffering providers can decorate active storage without losing access to the final durable provider.
- Added `IMuleBatchTerminalStorage` so durable providers can persist terminal state transitions in batches.
- Added FastLane InMemory integration coverage for buffered execution and ordered intent-before-terminal durable flushes.
- Added optional Redis integration coverage for buffered execution and ordered durable flushes.

### Changed

- Changed immediate dispatch and recovery workers to lock/claim actions, enqueue them into the lane executor, and continue admitting work instead of executing actions inline.
- Changed `WorkerCount` semantics to represent readers/claimers per lane while `MaxDegreeOfParallelism` controls actual concurrent execution.
- Changed recovery dispatch to pass claimed actions to the lane executor as a batch.
- Changed SQL Server completion, failure, and cleanup paths to use direct SQL statements instead of loading tracked entities for each operation.
- Changed EF Core indexes to include status-based locked recovery and completed cleanup paths.
- Changed FastLane Redis claim and completion paths to use Lua scripts and batched flush confirmation to reduce Redis round trips.

## [v1.3.0] - 2026-08-16

### Added

- Added weighted fair lane scheduling so priority influences throughput without starving lower-priority lanes.
- Added lane-specific immediate dispatch workers for configured lanes, plus fallback workers for unconfigured lanes.
- Added configurable drain cycles with `MaxDrainBatchesPerCycle`, `MaxDrainActionsPerCycle`, `DrainUntilEmpty`, and `YieldBetweenDrainBatches`.
- Added lane-level drain overrides on `MuleLaneSettings`.
- Added `MuleLaneSettings.Weight` for weighted scheduling.
- Added `ConfigureHighThroughputRuntime()` and `ConfigureLane(...)` registration helpers.
- Added runtime completed-per-minute and failure metrics by lane and action key.
- Added in-memory integration coverage for independent immediate lane workers and drain-until-empty recovery.

### Changed

- Changed immediate dispatch queue draining to use weighted round-robin lane scans instead of strict priority scans.
- Changed recovery cycles to drain configured batches continuously when enabled instead of always claiming a single batch per cycle.

## [v1.2.0] - 2026-08-15

### Added

- Added durable action lanes with lane-level worker, batch, parallelism, retry, and priority settings.
- Added `EnqueueOptions.Lane` and `MuleActionContext.Lane`.
- Added lane-level polling intervals for recovery polling.
- Added lane-level immediate dispatch queue capacity.
- Added concurrent dispatcher workers with configurable maximum parallelism.
- Added extended diagnostics for expired locks, duplicate enqueues, backlog, retries, failures, and latency.
- Added `StartedOnUtc` and `TerminalOnUtc` tracking for latency diagnostics.
- Added `MuleRetryPolicy` with fixed, linear, and exponential backoff plus max delay and jitter.
- Added runtime metrics for completed actions, failed actions, ignored duplicates, and recent throughput.
- Added SQL Server upgrade script for the v1.2.0 storage changes.
- Added optional SQL Server integration tests for concurrent deduplication and replica-style atomic claims.
- Added high-volume in-memory dispatcher stress coverage.

### Changed

- Changed pending recovery to claim and lock batches atomically before execution.
- Changed the in-memory dispatch queue to support multiple concurrent readers.
- Changed immediate dispatch queues to be isolated per lane and drained by lane priority.
- Changed duplicate enqueues to return the existing durable action id when a deduplication match is found.
- Changed EF Core storage to use a unique deduplication index for `ActionKey` and `DeduplicationKey`.
- Changed SQL Server batch claims to use an ordered CTE with `UPDLOCK`, `READPAST`, `ROWLOCK`, and `OUTPUT INSERTED`.
- Changed lane priority handling so higher-priority lanes claim work before lower-priority lanes.

## [v1.1.1] - 2026-08-09

### Added

- Added `Mule.DurableActions.Testing` package with an in-memory Mule test harness.
- Added testing sample project showing `UseTesting()` and `IMuleTestHarness`.

## [v1.1.0] - 2026-08-09

### Added

- Added chainable provider configuration through the Mule registration builder.
- Added scheduled recovery mode for retrying pending work without constant storage polling.
- Added configurable cleanup modes: disabled, polling, and scheduled.
- Added automatic EF Core model integration through `UseEntityFrameworkCore<TDbContext>()`.

### Changed

- Changed EF Core locking to use atomic conditional claims for safer execution across multiple service replicas.
- Updated documentation and sample code to use the chainable Mule configuration style.

## [v1.0.1] - 2026-08-08

### Fixed

- Fixed release automation so the merged `main` build is validated before publishing NuGet packages.

## [v1.0.0] - 2026-08-08

### Added

- Added `Mule.DurableActions` core package with `ActionKey`, `IMuleClient`, `IMuleAction<TPayload>`, `MuleActionContext<TPayload>`, and durable action execution contracts.
- Added `[MuleAction]` attribute and assembly discovery through `AddActionsFromAssembly(...)` and `AddActionsFromAssemblyContaining<TMarker>()`.
- Added hosted background dispatcher with immediate dispatch, polling recovery, lock timeout recovery, retries, failed state, completed state, and cleanup.
- Added JSON payload serialization through `IMuleSerializer` and `JsonMuleSerializer`.
- Added enqueue metadata support through `EnqueueOptions`, including correlation ID, deduplication key, and metadata.
- Added diagnostics contracts through `IMuleDiagnostics` and `MuleDiagnosticsSnapshot`.
- Added `Mule.DurableActions.InMemory` provider with process-local storage and `IInMemoryMule` inspection support.
- Added `Mule.DurableActions.EntityFrameworkCore` provider with `MuleDbContext`, `UseEntityFrameworkMule(...)`, and `UseMuleModel()`.
- Added embedded NuGet package icon.
- Added basic sample application showing action discovery, in-memory storage, enqueue, background execution, and completed status inspection.
