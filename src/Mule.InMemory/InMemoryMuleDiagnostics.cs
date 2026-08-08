namespace Mule.InMemory;

using Mule.Diagnostics;

internal sealed class InMemoryMuleDiagnostics : IMuleDiagnostics
{
    private readonly InMemoryMuleStore _store;

    public InMemoryMuleDiagnostics(InMemoryMuleStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
    }

    public Task<MuleDiagnosticsSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(_store.GetSnapshot());
}
