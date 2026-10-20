namespace Mule;

public sealed class MuleReplayRequest
{
    public required Ulid RequestId { get; init; }

    public string RequestedBy { get; init; }

    public string Reason { get; init; }

    public DateTimeOffset? ScheduleOnUtc { get; init; }

    public IReadOnlyDictionary<string, string> Metadata { get; init; }

    public bool AllowParallelReplay { get; init; }
}
