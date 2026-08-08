# Changelog

All notable changes to Mule packages will be documented in this file.

## [1.0.0] - 2026-08-08

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
