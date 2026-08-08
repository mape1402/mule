namespace Mule.Tests;

public sealed class ActionKeyTests
{
    [Fact]
    public void From_Should_Create_Key_For_Valid_Value()
    {
        var key = ActionKey.From("billing.capture-payment.v1");

        Assert.Equal("billing.capture-payment.v1", key.Value);
        Assert.Equal("billing.capture-payment.v1", key.ToString());
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("bad key")]
    [InlineData(".bad")]
    public void From_Should_Reject_Invalid_Value(string value)
    {
        Assert.Throws<ArgumentException>(() => ActionKey.From(value));
    }

    [Fact]
    public void Equals_Should_Use_Ordinal_Value()
    {
        Assert.Equal(ActionKey.From("a.b"), ActionKey.From("a.b"));
        Assert.NotEqual(ActionKey.From("a.b"), ActionKey.From("A.b"));
    }
}
