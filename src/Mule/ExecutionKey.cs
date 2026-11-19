namespace Mule;

public readonly struct ExecutionKey : IEquatable<ExecutionKey>
{
    public ExecutionKey(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException("Execution key cannot be null, empty, or whitespace.", nameof(value));
        if (value.Length > 512)
            throw new ArgumentException("Execution key cannot exceed 512 characters.", nameof(value));

        Value = value;
    }

    public string Value { get; }

    public static ExecutionKey From(string value)
        => new(value);

    public bool Equals(ExecutionKey other)
        => string.Equals(Value, other.Value, StringComparison.Ordinal);

    public override bool Equals(object obj)
        => obj is ExecutionKey other && Equals(other);

    public override int GetHashCode()
        => StringComparer.Ordinal.GetHashCode(Value ?? string.Empty);

    public override string ToString()
        => Value;

    public static bool operator ==(ExecutionKey left, ExecutionKey right)
        => left.Equals(right);

    public static bool operator !=(ExecutionKey left, ExecutionKey right)
        => !left.Equals(right);
}
