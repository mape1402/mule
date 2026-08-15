namespace Mule;

public sealed class EnqueueOptions
{
    public string Lane { get; set; } = MuleSettings.DefaultLane;

    public string CorrelationId { get; set; }

    public string DeduplicationKey { get; set; }

    public IDictionary<string, string> Metadata { get; } = new Dictionary<string, string>();
}
