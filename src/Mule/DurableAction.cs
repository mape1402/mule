namespace Mule;

public sealed class DurableAction
{
    public Guid Id { get; set; } = Guid.NewGuid();

    public ActionKey Key { get; set; }

    public string Lane { get; set; } = MuleSettings.DefaultLane;

    public string Payload { get; set; }

    public string PayloadType { get; set; }

    public string Metadata { get; set; }

    public string CorrelationId { get; set; }

    public string DeduplicationKey { get; set; }

    public DurableActionStatus Status { get; set; } = DurableActionStatus.Pending;

    public int Attempts { get; set; }

    public string LastError { get; set; }

    public DateTimeOffset CreatedOnUtc { get; set; } = DateTimeOffset.UtcNow;

    public DateTimeOffset? LockedOnUtc { get; set; }

    public DateTimeOffset? StartedOnUtc { get; set; }

    public DateTimeOffset? NextAttemptOnUtc { get; set; }

    public DateTimeOffset? CompletedOnUtc { get; set; }

    public DateTimeOffset? TerminalOnUtc { get; set; }
}
