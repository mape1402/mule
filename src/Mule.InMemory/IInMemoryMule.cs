namespace Mule.InMemory;

public interface IInMemoryMule
{
    IReadOnlyCollection<DurableAction> Actions { get; }
}
