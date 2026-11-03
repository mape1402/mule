namespace Mule.Redis;

using System.Text.Json;
using StackExchange.Redis;

internal static class RedisMuleReplayCodec
{
    public static MuleReplay FromHash(HashEntry[] entries)
    {
        if (entries == null || entries.Length == 0)
            return null;

        var values = entries.ToDictionary(x => (string)x.Name, x => x.Value, StringComparer.Ordinal);
        return new MuleReplay
        {
            RequestId = Ulid.Parse(Required(values, "id")),
            ConcurrencyToken = Ulid.Parse(Required(values, "ct")),
            SourceActionId = Ulid.Parse(Required(values, "source")),
            ReplayRootActionId = Ulid.Parse(Required(values, "root")),
            ReplayActionId = Ulid.Parse(Required(values, "action")),
            ReplaySequence = Integer(values, "sequence"),
            BranchId = Ulid.Parse(Required(values, "branch")),
            ActionKey = ActionKey.From(Required(values, "key")),
            Lane = Value(values, "lane"),
            CorrelationId = Value(values, "correlation"),
            OriginalDeduplicationKey = Value(values, "originalDedupe"),
            Status = (MuleReplayStatus)Integer(values, "status"),
            RequestedBy = Value(values, "requestedBy"),
            Reason = Value(values, "reason"),
            Metadata = Value(values, "metadata"),
            RequestedOnUtc = Date(values, "requested") ?? DateTimeOffset.UnixEpoch,
            ScheduleOnUtc = Date(values, "schedule") ?? DateTimeOffset.UnixEpoch,
            Attempts = Integer(values, "attempts"),
            NextAttemptOnUtc = Date(values, "next"),
            LastError = Value(values, "error"),
            LockedOnUtc = Date(values, "locked"),
            LockToken = ParseUlid(Value(values, "lockToken")),
            MaterializedOnUtc = Date(values, "materialized"),
            CancelledOnUtc = Date(values, "cancelled")
        };
    }

    public static MuleReplayInfo ToInfo(MuleReplay replay)
    {
        if (replay == null)
            return null;

        return new MuleReplayInfo
        {
            RequestId = replay.RequestId,
            ReplayOfActionId = replay.SourceActionId,
            ReplayRootActionId = replay.ReplayRootActionId,
            ReplayActionId = replay.ReplayActionId,
            ReplaySequence = replay.ReplaySequence,
            Status = replay.Status,
            ActionKey = replay.ActionKey,
            Lane = replay.Lane,
            CorrelationId = replay.CorrelationId,
            OriginalDeduplicationKey = replay.OriginalDeduplicationKey,
            RequestedOnUtc = replay.RequestedOnUtc,
            ScheduleOnUtc = replay.ScheduleOnUtc,
            RequestedBy = replay.RequestedBy,
            Reason = replay.Reason,
            Metadata = DeserializeMetadata(replay.Metadata),
            MaterializationAttempts = replay.Attempts,
            LastError = replay.LastError,
            MaterializedOnUtc = replay.MaterializedOnUtc,
            CancelledOnUtc = replay.CancelledOnUtc
        };
    }

    public static string SerializeMetadata(IReadOnlyDictionary<string, string> metadata)
    {
        if (metadata == null || metadata.Count == 0)
            return null;

        var canonical = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (var item in metadata)
            canonical[item.Key] = item.Value;
        return JsonSerializer.Serialize(canonical);
    }

    private static IReadOnlyDictionary<string, string> DeserializeMetadata(string metadata)
        => string.IsNullOrWhiteSpace(metadata)
            ? new Dictionary<string, string>()
            : JsonSerializer.Deserialize<Dictionary<string, string>>(metadata) ?? new Dictionary<string, string>();

    private static Ulid ParseUlid(string value)
        => Ulid.TryParse(value, out var parsed) ? parsed : Ulid.Empty;

    private static string Required(IReadOnlyDictionary<string, RedisValue> values, string name)
        => Value(values, name) ?? throw new InvalidOperationException($"Redis Mule replay is missing field '{name}'.");

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
}
