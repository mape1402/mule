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

    [Fact]
    public void AddMule_Should_Register_Action_Types_Without_Adding_Them_To_DI()
    {
        var services = new ServiceCollection();
        var key = ActionKey.From("action.type");

        services.AddMule(mule =>
        {
            mule.For<TestAction, TestPayload>(key);
        });

        using var provider = services.BuildServiceProvider();

        Assert.Null(provider.GetService<TestAction>());
    }

    private sealed class TestService
    {
    }

    private sealed record TestPayload(string Value);

    private sealed class TestAction : IMuleAction<TestPayload>
    {
        public ValueTask ExecuteAsync(MuleActionContext<TestPayload> context, CancellationToken cancellationToken)
            => ValueTask.CompletedTask;
    }
}
