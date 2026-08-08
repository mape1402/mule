namespace Mule;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class MuleActionAttribute : Attribute
{
    public MuleActionAttribute(string key)
    {
        Key = ActionKey.From(key);
    }

    public ActionKey Key { get; }
}
