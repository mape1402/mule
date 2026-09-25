# Mule Nice To Have

These items are not required for the current Redis connection change, but they are useful extension points to keep visible as Mule grows.

## Payload Serialization

Mule should eventually allow applications to customize payload serialization.

Possible shape:

- `JsonSerializerOptions` for common System.Text.Json customization.
- `IMulePayloadSerializer` for full control.

Why it matters:

- Custom converters.
- Value objects.
- `DateOnly` / `TimeOnly`.
- Polymorphic payloads.
- Payload versioning.
- Optional compression or encryption.

## Time Provider

Mule should eventually allow a custom clock/time provider.

Possible shape:

- `TimeProvider`.
- Or a small `IMuleClock` abstraction if supporting older target shapes requires it.

Why it matters:

- Deterministic tests.
- Retry and cleanup testing without real sleeps.
- Easier validation of lock expiration and scheduled recovery.

## Retry / Backoff Strategy

Mule should eventually allow richer retry policies beyond simple fixed configuration.

Possible shape:

- `IMuleRetryStrategy`.
- Or a delegate that receives action context, attempt count, and exception/error state.

Why it matters:

- Exponential backoff.
- Jitter.
- Error-specific retry behavior.
- Per-action retry rules.
