namespace Mule.Configuration;

public static class MuleHighThroughputRegistrationBuilderExtensions
{
    public static IMuleRegistrationBuilder ConfigureHighThroughputRuntime(this IMuleRegistrationBuilder builder)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));

        return builder.Configure(settings =>
        {
            settings.RecoveryMode = MuleRecoveryMode.Scheduled;
            settings.ImmediateDispatch = true;
            settings.WorkerCount = Math.Max(settings.WorkerCount, Environment.ProcessorCount);
            settings.MaxDegreeOfParallelism = Math.Max(settings.MaxDegreeOfParallelism, Environment.ProcessorCount * 8);
            settings.DispatchBatchSize = Math.Max(settings.DispatchBatchSize, 250);
            settings.MaxDrainBatchesPerCycle = Math.Max(settings.MaxDrainBatchesPerCycle, 8);
            settings.MaxDrainActionsPerCycle = Math.Max(settings.MaxDrainActionsPerCycle, 2_000);
            settings.YieldBetweenDrainBatches = settings.YieldBetweenDrainBatches <= TimeSpan.Zero
                ? TimeSpan.FromMilliseconds(1)
                : settings.YieldBetweenDrainBatches;
            settings.DispatchInterval = settings.DispatchInterval <= TimeSpan.Zero || settings.DispatchInterval > TimeSpan.FromSeconds(5)
                ? TimeSpan.FromSeconds(5)
                : settings.DispatchInterval;
        });
    }

    public static IMuleRegistrationBuilder ConfigureLane(
        this IMuleRegistrationBuilder builder,
        string lane,
        Action<MuleLaneSettings> configure)
    {
        if (builder == null)
            throw new ArgumentNullException(nameof(builder));
        if (string.IsNullOrWhiteSpace(lane))
            throw new ArgumentException("A lane name is required.", nameof(lane));
        if (configure == null)
            throw new ArgumentNullException(nameof(configure));

        return builder.Configure(settings =>
        {
            if (!settings.Lanes.TryGetValue(lane, out var laneSettings))
            {
                laneSettings = new MuleLaneSettings();
                settings.Lanes[lane] = laneSettings;
            }

            configure(laneSettings);
        });
    }
}
