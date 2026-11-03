namespace Mule.Redis.Tests;

public sealed class RedisFactAttribute : FactAttribute
{
    public RedisFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(RedisMuleTests.ConnectionStringEnvironmentVariable)))
            Skip = $"Set {RedisMuleTests.ConnectionStringEnvironmentVariable} to run Redis integration tests.";
    }
}
