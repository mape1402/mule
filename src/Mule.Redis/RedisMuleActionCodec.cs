namespace Mule.Redis;

using StackExchange.Redis;

internal static class RedisMuleActionCodec
{
    public const int ArgumentCount = 22;

    public static RedisValue[] ToArguments(DurableAction action, RedisMuleKeyspace keyspace, TimeSpan lockTimeout)
    {
        var lane = NormalizeLane(action.Lane);
        var dueOnUtc = action.Status switch
        {
            DurableActionStatus.Locked => (action.LockedOnUtc ?? action.CreatedOnUtc).Add(lockTimeout),
            _ => action.NextAttemptOnUtc ?? action.CreatedOnUtc
        };

        return
        [
            action.Id.ToString(),
            action.ConcurrencyToken.ToString(),
            action.Key.Value ?? string.Empty,
            lane,
            keyspace.LaneDue(lane),
            action.Payload ?? string.Empty,
            action.PayloadType ?? string.Empty,
            action.Metadata ?? string.Empty,
            action.CorrelationId ?? string.Empty,
            action.DeduplicationKey ?? string.Empty,
            string.IsNullOrWhiteSpace(action.DeduplicationKey)
                ? string.Empty
                : keyspace.DedupeField(action.Key, action.DeduplicationKey),
            (int)action.Status,
            action.Attempts,
            action.LastError ?? string.Empty,
            Milliseconds(action.CreatedOnUtc),
            Milliseconds(action.LockedOnUtc),
            Milliseconds(action.StartedOnUtc),
            Milliseconds(action.NextAttemptOnUtc),
            Milliseconds(action.CompletedOnUtc),
            Milliseconds(action.CancelledOnUtc),
            Milliseconds(action.TerminalOnUtc),
            Milliseconds(dueOnUtc)
        ];
    }

    public static DurableAction FromHash(HashEntry[] entries)
    {
        if (entries == null || entries.Length == 0)
            return null;

        var values = entries.ToDictionary(x => (string)x.Name, x => x.Value, StringComparer.Ordinal);
        return new DurableAction
        {
            Id = Ulid.Parse(Required(values, "id")),
            ConcurrencyToken = Ulid.Parse(Required(values, "ct")),
            Key = ActionKey.From(Required(values, "key")),
            Lane = Value(values, "lane") ?? MuleSettings.DefaultLane,
            Payload = Value(values, "payload"),
            PayloadType = Value(values, "payloadType"),
            Metadata = Value(values, "metadata"),
            CorrelationId = Value(values, "correlation"),
            DeduplicationKey = Value(values, "dedupe"),
            Status = (DurableActionStatus)Integer(values, "status"),
            Attempts = Integer(values, "attempts"),
            LastError = Value(values, "error"),
            CreatedOnUtc = Date(values, "created") ?? DateTimeOffset.UnixEpoch,
            LockedOnUtc = Date(values, "locked"),
            StartedOnUtc = Date(values, "started"),
            NextAttemptOnUtc = Date(values, "next"),
            CompletedOnUtc = Date(values, "completed"),
            CancelledOnUtc = Date(values, "cancelled"),
            TerminalOnUtc = Date(values, "terminal")
        };
    }

    public static long Milliseconds(DateTimeOffset value)
        => value.ToUniversalTime().ToUnixTimeMilliseconds();

    public static string Milliseconds(DateTimeOffset? value)
        => value.HasValue ? Milliseconds(value.Value).ToString(System.Globalization.CultureInfo.InvariantCulture) : string.Empty;

    private static string Required(IReadOnlyDictionary<string, RedisValue> values, string name)
        => Value(values, name) ?? throw new InvalidOperationException($"Redis Mule action is missing field '{name}'.");

    private static string Value(IReadOnlyDictionary<string, RedisValue> values, string name)
    {
        if (!values.TryGetValue(name, out var value) || !value.HasValue || value.IsNullOrEmpty)
            return null;

        return value.ToString();
    }

    private static int Integer(IReadOnlyDictionary<string, RedisValue> values, string name)
        => int.TryParse(Value(values, name), out var value) ? value : 0;

    private static DateTimeOffset? Date(IReadOnlyDictionary<string, RedisValue> values, string name)
        => long.TryParse(Value(values, name), out var value)
            ? DateTimeOffset.FromUnixTimeMilliseconds(value)
            : null;

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;
}
