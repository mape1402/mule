namespace Mule.Dispatching;

public interface IMuleCommitNotifier
{
    ValueTask NotifySavedAsync(Guid actionId, string lane, CancellationToken cancellationToken = default);
}
