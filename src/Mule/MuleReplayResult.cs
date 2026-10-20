namespace Mule;

public sealed class MuleReplayResult
{
    public required MuleReplayOutcome Outcome { get; init; }

    public MuleReplayInfo Replay { get; init; }

    public Ulid OriginalActionId
    {
        get
        {
            if (Replay == null)
                return Ulid.Empty;

            return Replay.ReplayOfActionId;
        }
    }

    public Ulid ReplayActionId
    {
        get
        {
            if (Replay == null)
                return Ulid.Empty;

            return Replay.ReplayActionId;
        }
    }
}
