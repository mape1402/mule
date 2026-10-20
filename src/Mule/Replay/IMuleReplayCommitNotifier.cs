namespace Mule.Replay;

internal interface IMuleReplayCommitNotifier
{
    ValueTask NotifyScheduledAsync(DateTimeOffset scheduleOnUtc);
}
