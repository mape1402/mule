namespace Mule.Replay;

using System.Text.Json;

internal static class MuleReplayModel
{
    private const string ReplayDeduplicationPrefix = "mule:replay:";

    public static MuleReplayRequest NormalizeRequest(MuleReplayRequest request, DateTimeOffset now)
    {
        var scheduleOnUtc = request.ScheduleOnUtc?.ToUniversalTime() ?? now;
        if (scheduleOnUtc < now)
            scheduleOnUtc = now;

        return new MuleReplayRequest
        {
            RequestId = request.RequestId,
            RequestedBy = request.RequestedBy,
            Reason = request.Reason,
            ScheduleOnUtc = scheduleOnUtc,
            Metadata = DeserializeMetadata(SerializeMetadata(request.Metadata)),
            AllowParallelReplay = request.AllowParallelReplay
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

    public static IReadOnlyDictionary<string, string> DeserializeMetadata(string metadata)
    {
        if (string.IsNullOrWhiteSpace(metadata))
            return new Dictionary<string, string>();

        return JsonSerializer.Deserialize<Dictionary<string, string>>(metadata)
            ?? new Dictionary<string, string>();
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
            MaterializedOnUtc = replay.MaterializedOnUtc
        };
    }

    public static bool Matches(MuleReplay replay, Ulid sourceActionId, MuleReplayRequest request)
        => replay.SourceActionId == sourceActionId
            && string.Equals(replay.RequestedBy, request.RequestedBy, StringComparison.Ordinal)
            && string.Equals(replay.Reason, request.Reason, StringComparison.Ordinal)
            && replay.ScheduleOnUtc == request.ScheduleOnUtc
            && string.Equals(replay.Metadata, SerializeMetadata(request.Metadata), StringComparison.Ordinal)
            && replay.BranchId == GetBranchId(request);

    public static Ulid GetBranchId(MuleReplayRequest request)
        => request.AllowParallelReplay ? request.RequestId : Ulid.Empty;

    public static DurableAction CreateAction(MuleReplayClaim claim, DateTimeOffset now)
    {
        var source = claim.SourceAction;
        var replay = claim.Replay;

        return new DurableAction
        {
            Id = replay.ReplayActionId,
            ConcurrencyToken = Ulid.NewUlid(),
            Key = source.Key,
            Lane = source.Lane,
            Payload = source.Payload,
            PayloadType = source.PayloadType,
            Metadata = MergeMetadata(source.Metadata, replay.Metadata),
            CorrelationId = source.CorrelationId,
            DeduplicationKey = ReplayDeduplicationPrefix + replay.RequestId,
            Status = DurableActionStatus.Pending,
            Attempts = 0,
            CreatedOnUtc = now
        };
    }

    private static string MergeMetadata(string original, string enrichment)
    {
        if (string.IsNullOrWhiteSpace(enrichment))
            return original;

        var merged = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (var item in DeserializeMetadata(original))
            merged[item.Key] = item.Value;
        foreach (var item in DeserializeMetadata(enrichment))
            merged[item.Key] = item.Value;

        return merged.Count == 0 ? null : JsonSerializer.Serialize(merged);
    }
}
