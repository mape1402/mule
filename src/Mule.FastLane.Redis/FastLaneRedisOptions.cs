namespace Mule.FastLane.Redis;

public sealed class FastLaneRedisOptions
{
    public string ConnectionString { get; set; } = "localhost:6379";

    public string KeyPrefix { get; set; } = "mule";

    public int Database { get; set; } = -1;

    public int IntentFlushSize { get; set; } = 500;

    public int CompletionFlushSize { get; set; } = 1_000;

    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromMilliseconds(50);

    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(2);

    public TimeSpan DeduplicationRetention { get; set; } = TimeSpan.FromDays(7);
}
