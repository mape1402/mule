namespace Mule.Dispatching;

public interface IMuleActionRegistry
{
    bool TryGet(ActionKey key, out object handler);
}
