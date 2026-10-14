namespace Mule.Tests;

using System.Text.Json;

public sealed class UlidTests
{
    [Fact]
    public void NewUlid_Should_Create_Parseable_26_Character_Value()
    {
        var id = Ulid.NewUlid();

        Assert.True(Ulid.TryParse(id.ToString(), out var parsed));
        Assert.Equal(26, id.ToString().Length);
        Assert.Equal(id, parsed);
    }

    [Fact]
    public void Parse_Should_Normalize_Crockford_Aliases()
    {
        var id = Ulid.Parse("01arylz7tszzzzzzzzzzzzzzzz");

        Assert.Equal("01ARY1Z7TSZZZZZZZZZZZZZZZZ", id.ToString());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("bad")]
    [InlineData("81ARYZ6S41YYYYYYYYYYYYYYYY")]
    [InlineData("01ARYZ6S41YYYYYYYYYYYYYYY!")]
    public void TryParse_Should_Reject_Invalid_Values(string value)
    {
        Assert.False(Ulid.TryParse(value, out _));
    }

    [Fact]
    public void Json_Should_Roundtrip_As_String()
    {
        var id = Ulid.NewUlid();

        var json = JsonSerializer.Serialize(id);
        var parsed = JsonSerializer.Deserialize<Ulid>(json);

        Assert.Equal($"\"{id}\"", json);
        Assert.Equal(id, parsed);
    }

    [Fact]
    public void CompareTo_Should_Use_Lexical_Order()
    {
        var older = Ulid.Parse("01ARYZ6S410000000000000000");
        var newer = Ulid.Parse("01ARYZ6S420000000000000000");

        Assert.True(older < newer);
        Assert.True(newer > older);
        Assert.True(older <= Ulid.Parse("01ARYZ6S410000000000000000"));
        Assert.True(newer >= older);
        Assert.NotEqual(older, newer);
    }
}
