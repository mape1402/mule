# Changelog

All notable changes to Mule packages will be documented in this file.

## [v1.1.0] - 2026-08-09

### Added

- Added chainable provider configuration through the Mule registration builder.
- Added scheduled recovery mode for retrying pending work without constant storage polling.
- Added configurable cleanup modes: disabled, polling, and scheduled.
- Added automatic EF Core model integration through `UseEntityFrameworkCore<TDbContext>()`.
- Added `Mule.DurableActions.Testing` package with an in-memory Mule test harness.
- Added testing sample project showing `UseTesting()` and `IMuleTestHarness`.

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
