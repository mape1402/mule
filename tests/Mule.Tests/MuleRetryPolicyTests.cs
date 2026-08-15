namespace Mule.Tests;

public sealed class MuleRetryPolicyTests
{
    [Fact]
    public void GetDelay_Should_Use_Fixed_Backoff_By_Default()
    {
        var policy = new MuleRetryPolicy
        {
            Delay = TimeSpan.FromSeconds(5)
        };

        Assert.Equal(TimeSpan.FromSeconds(5), policy.GetDelay(1));
        Assert.Equal(TimeSpan.FromSeconds(5), policy.GetDelay(3));
    }

    [Fact]
    public void GetDelay_Should_Use_Linear_Backoff()
    {
        var policy = new MuleRetryPolicy
        {
            Delay = TimeSpan.FromSeconds(5),
            Backoff = MuleRetryBackoff.Linear
        };

        Assert.Equal(TimeSpan.FromSeconds(5), policy.GetDelay(1));
        Assert.Equal(TimeSpan.FromSeconds(15), policy.GetDelay(3));
    }

    [Fact]
    public void GetDelay_Should_Use_Exponential_Backoff_With_MaxDelay()
    {
        var policy = new MuleRetryPolicy
        {
            Delay = TimeSpan.FromSeconds(5),
            MaxDelay = TimeSpan.FromSeconds(30),
            Backoff = MuleRetryBackoff.Exponential
        };

        Assert.Equal(TimeSpan.FromSeconds(5), policy.GetDelay(1));
        Assert.Equal(TimeSpan.FromSeconds(20), policy.GetDelay(3));
        Assert.Equal(TimeSpan.FromSeconds(30), policy.GetDelay(5));
    }
}
