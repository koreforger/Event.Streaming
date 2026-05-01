using System.Text.Json;

namespace Event.Streaming.Processing.Envelopes;

/// <summary>
/// Shared stable envelope carried by all processed messages through the platform.
/// The envelope metadata is always well-typed; the payload is flexible.
/// </summary>
public interface IOperationalEnvelope
{
    Guid EnvelopeId { get; }
    string SchemaVersion { get; }

    // Source identity
    string SourceTopic { get; }
    int SourcePartition { get; }
    long SourceOffset { get; }
    DateTimeOffset SourceTimestamp { get; }

    // Processing identity
    DateTimeOffset IngestedTimestamp { get; }
    string ProducerApp { get; }
    string ProducerInstance { get; }
    string ClassificationProfile { get; }

    // Payload
    string PayloadContentType { get; }
    JsonElement Payload { get; }

    // Routing + correlation
    IReadOnlyDictionary<string, object> RoutingTags { get; }
    IReadOnlyList<string> ProcessingWarnings { get; }
    string CorrelationId { get; }
    string? TraceId { get; }
}
