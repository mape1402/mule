namespace Mule.Dispatching;

using Microsoft.Extensions.Options;

internal sealed class MuleExecutionKeyCoordinator
{
    private readonly object _gate = new();
    private readonly HashSet<ExecutionKey> _active = new();
    private readonly MuleSchedulerSignal _schedulerSignal;
    private readonly MuleSettings _settings;

    public MuleExecutionKeyCoordinator(
        MuleSchedulerSignal schedulerSignal,
        IOptions<MuleSettings> settings)
    {
        _schedulerSignal = schedulerSignal ?? throw new ArgumentNullException(nameof(schedulerSignal));
        _settings = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
    }

    public bool IsEnabled => _settings.ExecutionKeySelector != null;

    public bool TryAcquire(
        DurableAction action,
        out MuleExecutionKeyLease lease,
        out ExecutionKey? executionKey)
    {
        ArgumentNullException.ThrowIfNull(action);
        lease = null;
        executionKey = _settings.ExecutionKeySelector?.Invoke(action);
        if (executionKey == null)
            return true;
        if (string.IsNullOrWhiteSpace(executionKey.Value.Value))
            throw new InvalidOperationException(
                "ExecutionKeySelector returned a default or empty execution key.");

        lock (_gate)
        {
            if (!_active.Add(executionKey.Value))
                return false;
        }

        lease = new MuleExecutionKeyLease(this, executionKey.Value);
        return true;
    }

    private void Release(ExecutionKey key)
    {
        lock (_gate)
            _active.Remove(key);

        _schedulerSignal.SignalRecovery();
    }

    internal sealed class MuleExecutionKeyLease : IDisposable
    {
        private MuleExecutionKeyCoordinator _owner;

        public MuleExecutionKeyLease(MuleExecutionKeyCoordinator owner, ExecutionKey key)
        {
            _owner = owner;
            Key = key;
        }

        public ExecutionKey Key { get; }

        public void Dispose()
            => Interlocked.Exchange(ref _owner, null)?.Release(Key);
    }
}
