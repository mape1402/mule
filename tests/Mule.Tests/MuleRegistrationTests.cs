namespace Mule.Tests;

using Microsoft.Extensions.DependencyInjection;

public sealed class MuleRegistrationTests
{
    [Fact]
    public void AddMule_Should_Reject_Duplicate_Action_Keys()
    {
        var services = new ServiceCollection();
        var key = ActionKey.From("duplicate.key");

        Assert.Throws<InvalidOperationException>(() =>
            services.AddMule(mule =>
            {
                mule.For<TestService, TestPayload>(key, static (_, _, _) => ValueTask.CompletedTask);
                mule.For<TestService, TestPayload>(key, static (_, _, _) => ValueTask.CompletedTask);
            }));
    }

    private sealed class TestService
    {
    }

    private sealed record TestPayload(string Value);
}
