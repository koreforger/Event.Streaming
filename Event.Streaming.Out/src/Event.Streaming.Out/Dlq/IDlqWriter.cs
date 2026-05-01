using Event.Streaming.Processing.Envelopes;

namespace Event.Streaming.Out.Dlq;

/// <summary>
/// DLQ is entirely optional in the streaming platform.
/// When not configured (EnableDlq=false in settings), the NullDlqWriter is used
/// and failed messages are simply counted and logged without dead-letter routing.
/// </summary>

/// <summary>
/// Minimum metadata attached to every DLQ message.
/// </summary>
public sealed class DlqMetadata
{
    public Guid DlqId { get; init; } = Guid.NewGuid();
    public string OriginalTopic { get; init; } = string.Empty;
    public int OriginalPartition { get; init; }
    public long OriginalOffset { get; init; }
    public string FailureReason { get; init; } = string.Empty;
    public string FailureCategory { get; init; } = string.Empty;
    public string? ExceptionType { get; init; }
    public string? ExceptionMessage { get; init; }
    public string ProcessorApp { get; init; } = string.Empty;
    public string ProcessorInstance { get; init; } = string.Empty;
    public DateTimeOffset FailedAt { get; init; } = DateTimeOffset.UtcNow;
    public int RetryCount { get; init; }
    public string? StageName { get; init; }
    public string? ProfileName { get; init; }
    public string? ScriptVersion { get; init; }
}

/// <summary>
/// Writes a failed envelope to the DLQ topic.
/// Implementations must be safe to call even if DLQ is disabled (NullDlqWriter).
/// </summary>
public interface IDlqWriter
{
    /// <summary>Whether DLQ writing is actually enabled.</summary>
    bool IsEnabled { get; }

    Task<bool> WriteAsync(IOperationalEnvelope envelope, DlqMetadata metadata, CancellationToken ct);
}

/// <summary>
/// No-op DLQ writer used when DLQ is disabled.
/// Simply counts and logs the failure without routing to a dead-letter topic.
/// </summary>
public sealed class NullDlqWriter : IDlqWriter
{
    public static readonly NullDlqWriter Instance = new();

    public bool IsEnabled => false;

    public Task<bool> WriteAsync(IOperationalEnvelope envelope, DlqMetadata metadata, CancellationToken ct)
        => Task.FromResult(false);
}
