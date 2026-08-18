namespace Mule;

public readonly record struct MuleCompletedAction(Guid Id, DateTimeOffset CompletedOnUtc);
