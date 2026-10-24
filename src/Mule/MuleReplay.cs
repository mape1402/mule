namespace Mule;

public sealed class MuleReplay
{
    public Ulid RequestId { get; set; }

    public Ulid ConcurrencyToken { get; set; } = Ulid.NewUlid();

    public Ulid SourceActionId { get; set; }

    public Ulid ReplayRootActionId { get; set; }

    public Ulid ReplayActionId { get; set; }

    public int ReplaySequence { get; set; }

    public Ulid BranchId { get; set; }

    public ActionKey ActionKey { get; set; }

    public string Lane { get; set; }

    public string CorrelationId { get; set; }

    public string OriginalDeduplicationKey { get; set; }

    public MuleReplayStatus Status { get; set; } = MuleReplayStatus.Scheduled;

    public string RequestedBy { get; set; }

    public string Reason { get; set; }

    public string Metadata { get; set; }

    public DateTimeOffset RequestedOnUtc { get; set; }

    public DateTimeOffset ScheduleOnUtc { get; set; }

    public int Attempts { get; set; }

    public DateTimeOffset? NextAttemptOnUtc { get; set; }

    public string LastError { get; set; }

    public DateTimeOffset? LockedOnUtc { get; set; }

    public Ulid LockToken { get; set; }

    public DateTimeOffset? MaterializedOnUtc { get; set; }

    public DateTimeOffset? CancelledOnUtc { get; set; }
}
