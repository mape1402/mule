namespace Mule.Dispatching;

public sealed record MuleDispatchItem(Ulid ActionId, string Lane);
