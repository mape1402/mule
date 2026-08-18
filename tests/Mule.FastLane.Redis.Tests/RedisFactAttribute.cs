namespace Mule.FastLane.Redis.Tests;

public sealed class RedisFactAttribute : FactAttribute
{
    public RedisFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(RedisFastLaneTests.ConnectionStringEnvironmentVariable)))
            Skip = $"Set {RedisFastLaneTests.ConnectionStringEnvironmentVariable} to run Redis integration tests.";
    }
}
