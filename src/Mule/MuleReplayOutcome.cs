namespace Mule;

public enum MuleReplayOutcome
{
    Scheduled = 0,
    AlreadyScheduled = 1,
    NotFound = 2,
    NotEligible = 3,
    RequestConflict = 4,
    ReplayAlreadyExists = 5
}
