namespace Mule.Redis;

using System.Security.Cryptography;
using System.Text;

internal sealed class RedisMuleKeyspace
{
    public RedisMuleKeyspace(string prefix)
    {
        var normalized = string.IsNullOrWhiteSpace(prefix) ? "mule" : prefix.Trim();
        var slot = Hash(normalized)[..16];
        Root = $"{{mule-{slot}}}:mule";
    }

    public string Root { get; }

    public string Schema => $"{Root}:schema";

    public string ActionsDue => $"{Root}:actions:due";

    public string ActionDedupe => $"{Root}:actions:dedupe";

    public string Lanes => $"{Root}:actions:lanes";

    public string Pending => Status(DurableActionStatus.Pending);

    public string Locked => Status(DurableActionStatus.Locked);

    public string Completed => Status(DurableActionStatus.Completed);

    public string Failed => Status(DurableActionStatus.Failed);

    public string Cancelled => Status(DurableActionStatus.Cancelled);

    public string BacklogByLane => $"{Root}:metrics:backlog:lane";

    public string BacklogByActionKey => $"{Root}:metrics:backlog:action";

    public string FailuresByActionKey => $"{Root}:metrics:failures:action";

    public string RetriesByActionKey => $"{Root}:metrics:retries:action";

    public string Latencies => $"{Root}:metrics:latencies";

    public string ReplayDue => $"{Root}:replays:due";

    public string ReplayByAction => $"{Root}:replays:by-action";

    public string ReplayBranches => $"{Root}:replays:branches";

    public string Action(Ulid id) => $"{Root}:action:{id}";

    public string Replay(Ulid requestId) => $"{Root}:replay:{requestId}";

    public string LaneDue(string lane) => $"{Root}:lane:{Hash(NormalizeLane(lane))}:due";

    public string DedupeField(ActionKey key, string deduplicationKey)
        => Hash($"{key.Value}\u001f{deduplicationKey}");

    public string ReplayBranchField(Ulid sourceActionId, Ulid branchId)
        => Hash($"{sourceActionId}\u001f{branchId}");

    private string Status(DurableActionStatus status)
        => $"{Root}:actions:status:{(int)status}";

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;

    private static string Hash(string value)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
}
