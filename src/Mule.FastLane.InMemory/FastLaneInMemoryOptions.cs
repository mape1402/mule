namespace Mule.FastLane.InMemory;

public sealed class FastLaneInMemoryOptions
{
    public int IntentFlushSize { get; set; } = 500;

    public int CompletionFlushSize { get; set; } = 1_000;

    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromMilliseconds(50);

    public FastLaneAcknowledge Acknowledge { get; set; } = FastLaneAcknowledge.AfterBufferWrite;
}
