namespace Mule;

public readonly struct DeferralReason : IEquatable<DeferralReason>
{
    public DeferralReason(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException("Deferral reason cannot be null, empty, or whitespace.", nameof(value));
        if (value.Length > 128)
            throw new ArgumentException("Deferral reason cannot exceed 128 characters.", nameof(value));

        Value = value;
    }

    public string Value { get; }

    public static DeferralReason From(string value)
        => new(value);

    public bool Equals(DeferralReason other)
        => string.Equals(Value, other.Value, StringComparison.Ordinal);

    public override bool Equals(object obj)
        => obj is DeferralReason other && Equals(other);

    public override int GetHashCode()
        => StringComparer.Ordinal.GetHashCode(Value ?? string.Empty);

    public override string ToString()
        => Value;

    public static bool operator ==(DeferralReason left, DeferralReason right)
        => left.Equals(right);

    public static bool operator !=(DeferralReason left, DeferralReason right)
        => !left.Equals(right);
}
