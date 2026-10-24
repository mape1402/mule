namespace Mule;

public enum DurableActionStatus
{
    Pending = 0,
    Locked = 1,
    Completed = 2,
    Failed = 3,
    Cancelled = 4
}
