using System.Text.Json;

namespace Event.Streaming.Processing.Envelopes;

/// <summary>
/// Default mutable implementation of IOperationalEnvelope, used by pipeline stages
/// to construct or modify the shared envelope during processing.
/// </summary>
public sealed class OperationalEnvelope : IOperationalEnvelope
{
    public Guid EnvelopeId { get; init; } = Guid.NewGuid();
    public string SchemaVersion { get; init; } = "1.0";

    public string SourceTopic { get; init; } = string.Empty;
    public int SourcePartition { get; init; }
    public long SourceOffset { get; init; }
    public DateTimeOffset SourceTimestamp { get; init; }

    public DateTimeOffset IngestedTimestamp { get; init; } = DateTimeOffset.UtcNow;
    public string ProducerApp { get; set; } = string.Empty;
    public string ProducerInstance { get; set; } = string.Empty;
    public string ClassificationProfile { get; set; } = string.Empty;

    public string PayloadContentType { get; set; } = "application/json";
    public JsonElement Payload { get; set; }

    public Dictionary<string, object> Tags { get; init; } = new();
    public IReadOnlyDictionary<string, object> RoutingTags => Tags;

    public List<string> Warnings { get; init; } = new();
    public IReadOnlyList<string> ProcessingWarnings => Warnings;

    public string CorrelationId { get; init; } = Guid.NewGuid().ToString("N");
    public string? TraceId { get; set; }
}
