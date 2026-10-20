namespace Mule;

public interface IMuleReplayStorage
{
    Task<MuleReplayResult> ScheduleAsync(
        Ulid sourceActionId,
        MuleReplayRequest request,
        DateTimeOffset now,
        CancellationToken cancellationToken = default);

    Task<MuleReplayInfo> FindAsync(Ulid requestId, CancellationToken cancellationToken = default);

    Task<IReadOnlyCollection<MuleReplayClaim>> ClaimScheduledAsync(
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default);

    Task<DateTimeOffset?> GetNextScheduledOnUtcAsync(
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default);

    Task<MuleReplayMaterializationOutcome> MaterializeAsync(
        MuleReplayClaim claim,
        DurableAction action,
        DateTimeOffset now,
        CancellationToken cancellationToken = default);

    Task ReleaseAsync(
        MuleReplayClaim claim,
        string error,
        DateTimeOffset nextAttemptOnUtc,
        CancellationToken cancellationToken = default);
}
