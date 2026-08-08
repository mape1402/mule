namespace Mule.InMemory;

internal sealed class InMemoryMule : IInMemoryMule
{
    private readonly InMemoryMuleStore _store;

    public InMemoryMule(InMemoryMuleStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
    }

    public IReadOnlyCollection<DurableAction> Actions => _store.Actions;
}
