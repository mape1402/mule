namespace Mule;

using System.Text.RegularExpressions;

public readonly struct ActionKey : IEquatable<ActionKey>
{
    private static readonly Regex ValidKey = new("^[a-zA-Z0-9][a-zA-Z0-9._:-]*$", RegexOptions.Compiled);

    public ActionKey(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException("Action key cannot be null, empty, or whitespace.", nameof(value));

        if (!ValidKey.IsMatch(value))
            throw new ArgumentException("Action key can only contain letters, numbers, dots, underscores, colons, and hyphens.", nameof(value));

        Value = value;
    }

    public string Value { get; }

    public static ActionKey From(string value)
        => new(value);

    public bool Equals(ActionKey other)
        => string.Equals(Value, other.Value, StringComparison.Ordinal);

    public override bool Equals(object obj)
        => obj is ActionKey other && Equals(other);

    public override int GetHashCode()
        => StringComparer.Ordinal.GetHashCode(Value ?? string.Empty);

    public override string ToString()
        => Value;

    public static bool operator ==(ActionKey left, ActionKey right)
        => left.Equals(right);

    public static bool operator !=(ActionKey left, ActionKey right)
        => !left.Equals(right);
}
