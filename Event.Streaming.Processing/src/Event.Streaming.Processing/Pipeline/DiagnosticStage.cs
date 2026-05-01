namespace Event.Streaming.Processing.Pipeline;

/// <summary>
/// Runtime diagnostic pipeline stages. Controlled via SQL settings at runtime —
/// no rebuild needed to switch between stages.
/// </summary>
public enum DiagnosticStage
{
    /// <summary>Run all stages end-to-end.</summary>
    FullPipeline,

    /// <summary>Consume from Kafka only; no further processing.</summary>
    KafkaOnly,

    /// <summary>Decode raw bytes only.</summary>
    DecodeOnly,

    /// <summary>Decode and classify; skip parse and downstream.</summary>
    ClassifyOnly,

    /// <summary>Decode, classify, and parse; skip enrich and downstream.</summary>
    ParseOnly,

    /// <summary>Run rules evaluation only (requires curated input).</summary>
    RulesOnly,

    /// <summary>Run output routing only; skip rules.</summary>
    OutputOnly,

    /// <summary>Run SQL write stages only.</summary>
    SqlOnly,

    /// <summary>Discard all output — useful for throughput benchmarking without I/O.</summary>
    NullOutput,
}
