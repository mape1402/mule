namespace Mule;

public readonly record struct MuleCompletedAction(Ulid Id, DateTimeOffset CompletedOnUtc);
