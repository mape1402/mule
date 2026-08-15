namespace Mule.Diagnostics;

public sealed record MuleRuntimeMetricsSnapshot(
    int Completed,
    int Failed,
    int DuplicatesIgnored,
    int ThroughputPerMinute);
