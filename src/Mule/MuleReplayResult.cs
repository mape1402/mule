namespace Mule;

public sealed class MuleReplayResult
{
    public required MuleReplayOutcome Outcome { get; init; }

    public MuleReplayInfo Replay { get; init; }

    public Ulid OriginalActionId => Replay?.ReplayOfActionId ?? Ulid.Empty;

    public Ulid ReplayActionId => Replay?.ReplayActionId ?? Ulid.Empty;
}
