namespace Mule.Diagnostics;

public sealed class MuleDiagnosticsSnapshot
{
    public int Pending { get; init; }

    public int Locked { get; init; }

    public int Completed { get; init; }

    public int Failed { get; init; }

    public DateTimeOffset? OldestPendingOnUtc { get; init; }

    public DateTimeOffset? OldestFailedOnUtc { get; init; }
}
