namespace Mule.Dispatching;

public interface IMuleCommitNotifier
{
    ValueTask NotifySavedAsync(Guid actionId, CancellationToken cancellationToken = default);
}
