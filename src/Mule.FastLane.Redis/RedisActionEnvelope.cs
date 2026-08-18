namespace Mule.FastLane.Redis;

internal sealed class RedisActionEnvelope
{
    public Guid Id { get; set; }

    public string Key { get; set; }

    public string Lane { get; set; }

    public string Payload { get; set; }

    public string PayloadType { get; set; }

    public string Metadata { get; set; }

    public string CorrelationId { get; set; }

    public string DeduplicationKey { get; set; }

    public DurableActionStatus Status { get; set; }

    public int Attempts { get; set; }

    public string LastError { get; set; }

    public DateTimeOffset CreatedOnUtc { get; set; }

    public DateTimeOffset? LockedOnUtc { get; set; }

    public DateTimeOffset? StartedOnUtc { get; set; }

    public DateTimeOffset? NextAttemptOnUtc { get; set; }

    public DateTimeOffset? CompletedOnUtc { get; set; }

    public DateTimeOffset? TerminalOnUtc { get; set; }

    public bool IntentPersisted { get; set; }

    public bool IntentDirty { get; set; } = true;

    public bool TerminalDirty { get; set; }

    public bool TerminalFlushed { get; set; }

    public DurableAction ToAction()
        => new()
        {
            Id = Id,
            Key = ActionKey.From(Key),
            Lane = string.IsNullOrWhiteSpace(Lane) ? MuleSettings.DefaultLane : Lane,
            Payload = Payload,
            PayloadType = PayloadType,
            Metadata = Metadata,
            CorrelationId = CorrelationId,
            DeduplicationKey = DeduplicationKey,
            Status = Status,
            Attempts = Attempts,
            LastError = LastError,
            CreatedOnUtc = CreatedOnUtc,
            LockedOnUtc = LockedOnUtc,
            StartedOnUtc = StartedOnUtc,
            NextAttemptOnUtc = NextAttemptOnUtc,
            CompletedOnUtc = CompletedOnUtc,
            TerminalOnUtc = TerminalOnUtc
        };

    public static RedisActionEnvelope FromAction(DurableAction action)
        => new()
        {
            Id = action.Id,
            Key = action.Key.Value,
            Lane = string.IsNullOrWhiteSpace(action.Lane) ? MuleSettings.DefaultLane : action.Lane,
            Payload = action.Payload,
            PayloadType = action.PayloadType,
            Metadata = action.Metadata,
            CorrelationId = action.CorrelationId,
            DeduplicationKey = action.DeduplicationKey,
            Status = action.Status,
            Attempts = action.Attempts,
            LastError = action.LastError,
            CreatedOnUtc = action.CreatedOnUtc,
            LockedOnUtc = action.LockedOnUtc,
            StartedOnUtc = action.StartedOnUtc,
            NextAttemptOnUtc = action.NextAttemptOnUtc,
            CompletedOnUtc = action.CompletedOnUtc,
            TerminalOnUtc = action.TerminalOnUtc
        };
}
