namespace Mule;

public interface IMuleStateProjectionStorage
{
    Task<IReadOnlyCollection<MuleActionStateProjectionResult>> ProjectStateRangeAsync(
        IReadOnlyCollection<MuleActionStateProjection> projections,
        CancellationToken cancellationToken = default);
}

public sealed record MuleActionStateProjection
{
    public MuleActionStateProjection(
        DurableAction action,
        Ulid? expectedConcurrencyToken,
        Ulid projectionToken = default)
    {
        Action = action ?? throw new ArgumentNullException(nameof(action));
        ExpectedConcurrencyToken = expectedConcurrencyToken;
        ProjectionToken = projectionToken == Ulid.Empty ? Ulid.NewUlid() : projectionToken;
    }

    public DurableAction Action { get; }

    public Ulid? ExpectedConcurrencyToken { get; }

    public Ulid ProjectionToken { get; }
}

public readonly record struct MuleActionStateProjectionResult(
    Ulid ActionId,
    MuleActionTransitionOutcome Outcome);
