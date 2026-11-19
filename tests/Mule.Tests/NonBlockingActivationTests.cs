namespace Mule.Tests;

using Microsoft.Extensions.Options;
using Mule.Diagnostics;
using Mule.Dispatching;

public sealed class NonBlockingActivationTests
{
    [Fact]
    public async Task Full_Immediate_Queue_Should_Coalesce_Activation_Without_Blocking_Producer()
    {
        var settings = Options.Create(new MuleSettings
        {
            DispatchQueueCapacity = 1,
            ImmediateDispatch = true,
            RecoveryMode = MuleRecoveryMode.Scheduled
        });
        var queue = new ChannelMuleDispatchQueue(settings);
        var signal = new MuleSchedulerSignal();
        var metrics = new MuleRuntimeMetrics();
        var notifier = new AmbientTransactionMuleCommitNotifier(queue, signal, settings, metrics: metrics);
        var first = Ulid.NewUlid();
        var omitted = Ulid.NewUlid();

        await notifier.NotifySavedAsync(first, MuleSettings.DefaultLane);
        var notification = notifier.NotifySavedAsync(omitted, MuleSettings.DefaultLane);

        Assert.True(notification.IsCompletedSuccessfully);
        await signal.WaitForRecoveryAsync(CancellationToken.None);
        Assert.Equal(first, (await queue.DequeueAsync(MuleSettings.DefaultLane)).ActionId);
        var snapshot = metrics.GetSnapshot(DateTimeOffset.UtcNow);
        Assert.Equal(1, snapshot.ActivationsOmitted);
        Assert.Equal(0, snapshot.ActivationsRecovered);
    }
}
