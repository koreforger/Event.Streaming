namespace Event.Streaming.Processing.WorkStore;

public interface IEventReaderWorkStore
{
    Task EnqueueClassifiedAsync(ClassifiedWorkItem item, CancellationToken ct);

    Task<IReadOnlyList<WorkLease>> LeaseShardBatchAsync(
        int shardId,
        int maxItems,
        TimeSpan leaseDuration,
        CancellationToken ct);

    Task MarkReadyToOutputAsync(
        long workItemId,
        byte[] outputPayload,
        CancellationToken ct);

    Task<IReadOnlyList<OutputLease>> LeaseOutputBatchAsync(
        int maxItems,
        TimeSpan leaseDuration,
        CancellationToken ct);

    Task MarkCompletedAsync(
        long workItemId,
        OutputWriteReceipt receipt,
        CancellationToken ct);

    Task MarkRetryPendingAsync(
        long workItemId,
        RetryReason reason,
        CancellationToken ct);

    Task MarkFailedAsync(
        long workItemId,
        string reason,
        CancellationToken ct);

    Task ReleaseExpiredLeasesAsync(CancellationToken ct);

    Task<ReplayPlan> CreateReplayPlanAsync(
        ReplayRequest request,
        CancellationToken ct);

    Task<UnstuckResult> RequeueAsync(
        UnstuckRequest request,
        CancellationToken ct);

    /// <summary>Returns a snapshot of store health metrics without acquiring an exclusive lock.</summary>
    WorkStoreMetricsSnapshot GetWorkStoreMetrics();

    /// <summary>Returns the set of runtime model versions referenced by non-terminal work items.</summary>
    IReadOnlySet<long> GetActiveRuntimeModelVersions();
}

public sealed record KafkaSourceIdentity(
    string SourceSystemId,
    string Topic,
    int Partition,
    long Offset,
    DateTimeOffset? KafkaTimestampUtc);

public sealed record ClassifiedWorkItem
{
    public ClassifiedWorkItem(
        long workItemId,
        KafkaSourceIdentity source,
        int functionId,
        long nedbankId,
        int shardId,
        long runtimeModelVersion,
        long functionVersion,
        long extractionScriptVersion,
        long ruleSetVersion,
        long outputRouteVersion,
        byte[] rawPayload,
        DateTimeOffset createdUtc)
    {
        WorkItemId = workItemId;
        Source = source;
        FunctionId = functionId;
        NedbankId = nedbankId;
        ShardId = shardId;
        RuntimeModelVersion = runtimeModelVersion;
        FunctionVersion = functionVersion;
        ExtractionScriptVersion = extractionScriptVersion;
        RuleSetVersion = ruleSetVersion;
        OutputRouteVersion = outputRouteVersion;
        RawPayload = rawPayload.ToArray();
        CreatedUtc = createdUtc;
    }

    public long WorkItemId { get; }
    public KafkaSourceIdentity Source { get; }
    public int FunctionId { get; }
    public long NedbankId { get; }
    public int ShardId { get; }
    public long RuntimeModelVersion { get; }
    public long FunctionVersion { get; }
    public long ExtractionScriptVersion { get; }
    public long RuleSetVersion { get; }
    public long OutputRouteVersion { get; }
    public byte[] RawPayload { get; }
    public DateTimeOffset CreatedUtc { get; }
}

public enum WorkState
{
    Classified,
    Processing,
    ReadyToOutput,
    Publishing,
    Completed,
    RetryPending,
    Failed,
    Suppressed,
}

public sealed record WorkLease(
    long WorkItemId,
    int ShardId,
    string LeaseOwnerId,
    DateTimeOffset LeaseExpiresUtc,
    int ProcessingAttempt,
    ClassifiedWorkItem Item);

public sealed record OutputLease(
    long WorkItemId,
    string LeaseOwnerId,
    DateTimeOffset LeaseExpiresUtc,
    int OutputAttempt,
    byte[] OutputPayload);

public sealed record RetryReason(
    string Category,
    string Message,
    DateTimeOffset RetryAfterUtc,
    WorkState RetryTargetState);

public sealed record OutputWriteReceipt(
    string Topic,
    int Partition,
    long Offset,
    DateTimeOffset PublishedUtc,
    string? ProducerReceipt);

public sealed record ReplayRequest(
    ReplayMode Mode,
    IReadOnlyList<string> Topics,
    IReadOnlyList<string> SourceSystems,
    DateTimeOffset? FromUtc,
    DateTimeOffset? ToUtc,
    bool ContinueForever,
    IReadOnlyList<int> FunctionIds,
    string Reason);

public enum ReplayMode
{
    FromKafka,
    FromStore,
    FromFailed,
}

public sealed record ReplayPlan(
    Guid ReplayId,
    ReplayRequest Request,
    IReadOnlyList<long> WorkItemIds,
    DateTimeOffset CreatedUtc);

public sealed record UnstuckRequest(
    IReadOnlyList<long> WorkItemIds,
    string RequestedBy,
    string Reason);

public sealed record UnstuckResult(
    int RequestedCount,
    int MovedCount,
    IReadOnlyList<long> SkippedWorkItemIds);

/// <summary>
/// Point-in-time snapshot of work store health metrics.
/// Covers backlog by state/shard, latencies, disk, checkpoint, and active runtime model versions.
/// </summary>
public sealed record WorkStoreMetricsSnapshot(
    IReadOnlyDictionary<WorkState, long> BacklogByState,
    IReadOnlyDictionary<int, long> BacklogByShard,
    IReadOnlySet<long> ActiveRuntimeModelVersions,
    TimeSpan? OldestUnfinishedAge,
    long DiskFreeBytes,
    long DiskTotalBytes,
    double AverageEnqueueLatencyMs,
    double AverageShardLeaseLatencyMs,
    double AverageOutputLeaseLatencyMs,
    DateTimeOffset? LastCheckpointTime);
