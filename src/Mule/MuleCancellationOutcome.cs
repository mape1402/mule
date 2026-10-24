namespace Mule;

public enum MuleCancellationOutcome
{
    Cancelled = 0,
    AlreadyCancelled = 1,
    TooLate = 2,
    NotFound = 3,
    NotSupported = 4
}
