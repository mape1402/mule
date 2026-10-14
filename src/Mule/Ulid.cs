namespace Mule;

using System.Security.Cryptography;
using System.Text.Json.Serialization;

[JsonConverter(typeof(UlidJsonConverter))]
public readonly struct Ulid : IComparable, IComparable<Ulid>, IEquatable<Ulid>
{
    private const string Alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    private const string EmptyValue = "00000000000000000000000000";
    private readonly string _value;

    private Ulid(string value)
    {
        _value = value;
    }

    public static Ulid Empty => new(EmptyValue);

    public static Ulid NewUlid()
    {
        Span<byte> random = stackalloc byte[10];
        RandomNumberGenerator.Fill(random);

        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        Span<char> chars = stackalloc char[26];
        EncodeTimestamp(timestamp, chars[..10]);
        EncodeRandomness(random, chars[10..]);
        return new Ulid(new string(chars));
    }

    public static Ulid Parse(string value)
    {
        if (!TryParse(value, out var ulid))
            throw new FormatException("Value is not a valid ULID.");

        return ulid;
    }

    public static bool TryParse(string value, out Ulid ulid)
    {
        ulid = default;

        if (string.IsNullOrWhiteSpace(value) || value.Length != 26)
            return false;

        Span<char> normalized = stackalloc char[26];
        for (var index = 0; index < value.Length; index++)
        {
            var character = Normalize(value[index]);
            if (Alphabet.IndexOf(character) < 0)
                return false;

            normalized[index] = character;
        }

        if (normalized[0] > '7')
            return false;

        ulid = new Ulid(new string(normalized));
        return true;
    }

    public int CompareTo(object obj)
    {
        if (obj == null)
            return 1;

        if (obj is not Ulid other)
            throw new ArgumentException($"Object must be of type {nameof(Ulid)}.", nameof(obj));

        return CompareTo(other);
    }

    public int CompareTo(Ulid other)
        => string.CompareOrdinal(ToString(), other.ToString());

    public bool Equals(Ulid other)
        => string.Equals(ToString(), other.ToString(), StringComparison.Ordinal);

    public override bool Equals(object obj)
        => obj is Ulid other && Equals(other);

    public override int GetHashCode()
        => ToString().GetHashCode(StringComparison.Ordinal);

    public override string ToString()
        => _value ?? EmptyValue;

    public static bool operator ==(Ulid left, Ulid right)
        => left.Equals(right);

    public static bool operator !=(Ulid left, Ulid right)
        => !left.Equals(right);

    public static bool operator <(Ulid left, Ulid right)
        => left.CompareTo(right) < 0;

    public static bool operator >(Ulid left, Ulid right)
        => left.CompareTo(right) > 0;

    public static bool operator <=(Ulid left, Ulid right)
        => left.CompareTo(right) <= 0;

    public static bool operator >=(Ulid left, Ulid right)
        => left.CompareTo(right) >= 0;

    private static void EncodeTimestamp(long timestamp, Span<char> destination)
    {
        for (var index = destination.Length - 1; index >= 0; index--)
        {
            destination[index] = Alphabet[(int)(timestamp & 31)];
            timestamp >>= 5;
        }
    }

    private static void EncodeRandomness(ReadOnlySpan<byte> randomness, Span<char> destination)
    {
        var bits = 0;
        var buffer = 0;
        var output = 0;

        foreach (var value in randomness)
        {
            buffer = (buffer << 8) | value;
            bits += 8;

            while (bits >= 5)
            {
                destination[output++] = Alphabet[(buffer >> (bits - 5)) & 31];
                bits -= 5;
            }
        }
    }

    private static char Normalize(char value)
        => char.ToUpperInvariant(value) switch
        {
            'I' or 'L' => '1',
            'O' => '0',
            var normalized => normalized
        };
}
