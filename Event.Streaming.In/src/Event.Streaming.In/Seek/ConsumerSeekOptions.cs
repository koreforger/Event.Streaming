namespace Event.Streaming.In.Seek;

/// <summary>
/// Controls how the consumer positions itself before starting to consume.
/// </summary>
public enum SeekMode
{
    /// <summary>Use the Kafka consumer profile StartMode (Latest/Earliest) — normal operation.</summary>
    None,

    /// <summary>Seek all assigned partitions to a specific absolute offset.</summary>
    FromOffset,

    /// <summary>Seek all assigned partitions to the first offset at or after the given timestamp.</summary>
    FromTimestamp,

    /// <summary>Consume a bounded range: start at an offset or timestamp, stop at another.</summary>
    Range,
}

/// <summary>
/// Seek and stop configuration loaded from SQL settings.
/// Allows replay of messages between two points in time or between two offsets.
///
/// Typical use cases:
///   - Reprocess all messages from 10am yesterday:  Mode=FromTimestamp, Start="2026-04-27T10:00:00Z"
///   - Reprocess a known offset range:              Mode=Range, Start=84000, Stop=92000
///   - Process records between 10am and 11am:       Mode=Range, Start="2026-04-27T10:00:00Z", Stop="2026-04-27T11:00:00Z"
///   - Start fresh at latest:                       Mode=None (default)
/// </summary>
public sealed class ConsumerSeekOptions
{
    /// <summary>Seek behaviour. None = use profile defaults.</summary>
    public SeekMode Mode { get; init; } = SeekMode.None;

    /// <summary>
    /// Start position. Either a long offset (e.g. "84000") or an ISO 8601 UTC datetime
    /// (e.g. "2026-04-27T10:00:00Z"). Parsed at startup. Empty or null = ignored.
    /// </summary>
    public string? StartOffsetOrTimestamp { get; init; }

    /// <summary>
    /// Optional stop position. Same format as Start. When set in Range mode,
    /// the consumer stops processing and exits cleanly after reaching this boundary.
    /// Empty or null = no stop boundary (run indefinitely).
    /// </summary>
    public string? StopOffsetOrTimestamp { get; init; }

    /// <summary>
    /// Parses StartOffsetOrTimestamp and returns it as a long offset if it is a plain integer,
    /// or as null if it is a timestamp (use <see cref="TryParseStartAsTimestamp"/> instead).
    /// </summary>
    public bool TryParseStartAsOffset(out long offset)
    {
        if (!string.IsNullOrWhiteSpace(StartOffsetOrTimestamp) &&
            long.TryParse(StartOffsetOrTimestamp, out offset))
            return true;
        offset = default;
        return false;
    }

    /// <summary>
    /// Parses StartOffsetOrTimestamp as a UTC DateTimeOffset.
    /// </summary>
    public bool TryParseStartAsTimestamp(out DateTimeOffset timestamp)
        => TryParseValue(StartOffsetOrTimestamp, out timestamp);

    /// <summary>
    /// Parses StopOffsetOrTimestamp as a long offset.
    /// </summary>
    public bool TryParseStopAsOffset(out long offset)
    {
        if (!string.IsNullOrWhiteSpace(StopOffsetOrTimestamp) &&
            long.TryParse(StopOffsetOrTimestamp, out offset))
            return true;
        offset = default;
        return false;
    }

    /// <summary>
    /// Parses StopOffsetOrTimestamp as a UTC DateTimeOffset.
    /// </summary>
    public bool TryParseStopAsTimestamp(out DateTimeOffset timestamp)
        => TryParseValue(StopOffsetOrTimestamp, out timestamp);

    /// <summary>
    /// Returns true if a stop boundary is configured (either offset or timestamp).
    /// </summary>
    public bool HasStopBoundary =>
        !string.IsNullOrWhiteSpace(StopOffsetOrTimestamp);

    private static bool TryParseValue(string? value, out DateTimeOffset result)
    {
        if (!string.IsNullOrWhiteSpace(value) &&
            DateTimeOffset.TryParse(value,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal |
                System.Globalization.DateTimeStyles.AdjustToUniversal,
                out result))
            return true;
        result = default;
        return false;
    }
}
