# Changelog

All notable changes to Mule packages will be documented in this file.

## [Unreleased]

### Added

- Added durable action lanes with lane-level worker, batch, parallelism, retry, and priority settings.
- Added `EnqueueOptions.Lane` and `MuleActionContext.Lane`.
- Added concurrent dispatcher workers with configurable maximum parallelism.
- Added extended diagnostics for expired locks, duplicate enqueues, backlog, retries, failures, and latency.
- Added `StartedOnUtc` and `TerminalOnUtc` tracking for latency diagnostics.
- Added `MuleRetryPolicy` with fixed, linear, and exponential backoff plus max delay and jitter.
- Added runtime metrics for completed actions, failed actions, ignored duplicates, and recent throughput.

### Changed

- Changed pending recovery to claim and lock batches atomically before execution.
- Changed the in-memory dispatch queue to support multiple concurrent readers.
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
