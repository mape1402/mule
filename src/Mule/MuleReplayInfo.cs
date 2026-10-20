namespace Mule;

public sealed class MuleReplayInfo
{
    public required Ulid RequestId { get; init; }

    public required Ulid ReplayOfActionId { get; init; }

    public required Ulid ReplayRootActionId { get; init; }

    public required Ulid ReplayActionId { get; init; }

    public required int ReplaySequence { get; init; }

    public required MuleReplayStatus Status { get; init; }

    public required ActionKey ActionKey { get; init; }

    public string Lane { get; init; }

    public string CorrelationId { get; init; }

    public string OriginalDeduplicationKey { get; init; }

    public required DateTimeOffset RequestedOnUtc { get; init; }

    public required DateTimeOffset ScheduleOnUtc { get; init; }

    public string RequestedBy { get; init; }

    public string Reason { get; init; }

    public IReadOnlyDictionary<string, string> Metadata { get; init; }

    public int MaterializationAttempts { get; init; }

    public string LastError { get; init; }

    public DateTimeOffset? MaterializedOnUtc { get; init; }
}
