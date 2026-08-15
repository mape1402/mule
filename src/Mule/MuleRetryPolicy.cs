namespace Mule;

public sealed class MuleRetryPolicy
{
    public int MaxAttempts { get; set; } = 10;

    public TimeSpan Delay { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan? MaxDelay { get; set; }

    public MuleRetryBackoff Backoff { get; set; } = MuleRetryBackoff.Fixed;

    public double JitterRatio { get; set; }

    public TimeSpan GetDelay(int nextAttempt)
    {
        var attempt = Math.Max(1, nextAttempt);
        var delay = Delay <= TimeSpan.Zero ? TimeSpan.FromSeconds(30) : Delay;

        delay = Backoff switch
        {
            MuleRetryBackoff.Linear => TimeSpan.FromTicks(delay.Ticks * attempt),
            MuleRetryBackoff.Exponential => TimeSpan.FromTicks(delay.Ticks * (long)Math.Pow(2, attempt - 1)),
            _ => delay
        };

        if (MaxDelay is { } maxDelay && maxDelay > TimeSpan.Zero && delay > maxDelay)
            delay = maxDelay;

        var jitterRatio = Math.Clamp(JitterRatio, 0, 1);
        if (jitterRatio > 0)
        {
            var jitterTicks = (long)(delay.Ticks * jitterRatio);
            var offsetTicks = Random.Shared.NextInt64(-jitterTicks, jitterTicks + 1);
            delay = TimeSpan.FromTicks(Math.Max(0, delay.Ticks + offsetTicks));
        }

        return delay;
    }
}
