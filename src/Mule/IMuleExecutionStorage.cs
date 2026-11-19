namespace Mule;

public interface IMuleExecutionStorage
{
    Task<IReadOnlyCollection<DurableAction>> ReadPendingCandidatesAsync(
        string lane,
        int offset,
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now,
        CancellationToken cancellationToken = default);

    Task<DurableAction> FindActionAsync(Ulid id, CancellationToken cancellationToken = default);

    Task<MuleActionTransitionOutcome> CompleteClaimAsync(
        Ulid id,
        Ulid expectedConcurrencyToken,
        DateTimeOffset completedOnUtc,
        CancellationToken cancellationToken = default);

    Task<MuleActionTransitionOutcome> FailClaimAsync(
        Ulid id,
        Ulid expectedConcurrencyToken,
        string error,
        DateTimeOffset now,
        DateTimeOffset? nextAttemptOnUtc,
        CancellationToken cancellationToken = default);

    Task<MuleActionTransitionOutcome> DeferClaimAsync(
        Ulid id,
        Ulid expectedConcurrencyToken,
        DateTimeOffset deferredOnUtc,
        DateTimeOffset notBeforeUtc,
        DeferralReason reason,
        CancellationToken cancellationToken = default);
}

public enum MuleActionTransitionOutcome
{
    Applied = 0,
    LeaseLost = 1,
    NotFound = 2
}
