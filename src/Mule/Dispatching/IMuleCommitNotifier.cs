namespace Mule.Dispatching;

public interface IMuleCommitNotifier
{
    ValueTask NotifySavedAsync(Ulid actionId, string lane, CancellationToken cancellationToken = default);
}
