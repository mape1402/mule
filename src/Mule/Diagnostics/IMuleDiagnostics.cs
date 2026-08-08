namespace Mule.Diagnostics;

public interface IMuleDiagnostics
{
    Task<MuleDiagnosticsSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default);
}
