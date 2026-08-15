namespace Mule.InMemory;

using Microsoft.Extensions.Options;
using Mule.Diagnostics;

internal sealed class InMemoryMuleDiagnostics : IMuleDiagnostics
{
    private readonly InMemoryMuleStore _store;
    private readonly MuleSettings _settings;
    private readonly MuleRuntimeMetrics _metrics;

    public InMemoryMuleDiagnostics(
        InMemoryMuleStore store,
        IOptions<MuleSettings> settings,
        MuleRuntimeMetrics metrics)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
    }

    public Task<MuleDiagnosticsSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(_store.GetSnapshot(GetLockTimeout(), _metrics.GetSnapshot(DateTimeOffset.UtcNow)));

    private TimeSpan GetLockTimeout()
        => _settings.LockTimeout <= TimeSpan.Zero ? TimeSpan.FromMinutes(5) : _settings.LockTimeout;
}
