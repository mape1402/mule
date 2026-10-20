namespace Mule;

public sealed class MuleReplayClaim
{
    public required MuleReplay Replay { get; init; }

    public required DurableAction SourceAction { get; init; }

    public required Ulid LockToken { get; init; }
}
