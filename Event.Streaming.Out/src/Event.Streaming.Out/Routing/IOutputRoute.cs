using Event.Streaming.Processing.Envelopes;

namespace Event.Streaming.Out.Routing;

/// <summary>
/// Result of writing an envelope to an output destination.
/// </summary>
public sealed class WriteResult
{
    public bool Success { get; init; }
    public string RouteName { get; init; } = string.Empty;
    public string? TargetTopic { get; init; }
    public long? ProducedOffset { get; init; }
    public string? ErrorMessage { get; init; }
    public TimeSpan Elapsed { get; init; }

    public static WriteResult Ok(string routeName, string topic, long? offset, TimeSpan elapsed) =>
        new() { Success = true, RouteName = routeName, TargetTopic = topic, ProducedOffset = offset, Elapsed = elapsed };

    public static WriteResult Fail(string routeName, string error, TimeSpan elapsed) =>
        new() { Success = false, RouteName = routeName, ErrorMessage = error, Elapsed = elapsed };
}

/// <summary>
/// Single output route definition. May be required (blocks commit on failure) or optional.
/// </summary>
public interface IOutputRoute
{
    string RouteName { get; }
    string TargetTopic { get; }

    /// <summary>
    /// When true, commit is blocked if this route fails.
    /// When false, failure is logged and metered but processing continues.
    /// </summary>
    bool IsRequired { get; }

    /// <summary>Optional predicate — null means always route.</summary>
    Func<IOperationalEnvelope, bool>? RoutingPredicate { get; }
}

/// <summary>
/// Writes an envelope to an output destination.
/// </summary>
public interface IOutputWriter
{
    Task<WriteResult> WriteAsync(IOperationalEnvelope envelope, CancellationToken ct);
}
