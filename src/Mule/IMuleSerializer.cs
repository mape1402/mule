namespace Mule;

public interface IMuleSerializer
{
    string Serialize<T>(T value);

    object Deserialize(string payload, Type payloadType);
}
