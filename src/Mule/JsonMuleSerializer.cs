namespace Mule;

using System.Text.Json;

public sealed class JsonMuleSerializer : IMuleSerializer
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public string Serialize<T>(T value)
        => JsonSerializer.Serialize(value, Options);

    public object Deserialize(string payload, Type payloadType)
        => JsonSerializer.Deserialize(payload, payloadType, Options);
}
