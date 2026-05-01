namespace Event.Streaming.Processing.Monitoring;

/// <summary>
/// Platform health status values.
/// </summary>
public enum HealthStatus
{
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

/// <summary>
/// Kafka consumer metrics snapshot.
/// </summary>
public sealed class KafkaMetrics
{
    public string ConsumerState { get; init; } = "unknown";
    public int AssignedPartitionCount { get; init; }
    public long TotalLag { get; init; }
    public IReadOnlyDictionary<int, long> LagPerPartition { get; init; } = new Dictionary<int, long>();
    public DateTimeOffset LastRebalance { get; init; }
    public int RebalanceCount { get; init; }
}

/// <summary>
/// Processing pipeline throughput and latency metrics.
/// </summary>
public sealed class PipelineMetrics
{
    public long TotalProcessed { get; init; }
    public long TotalErrors { get; init; }
    public double AvgLatencyMs { get; init; }
    public double P95LatencyMs { get; init; }
    public double P99LatencyMs { get; init; }
    public IReadOnlyDictionary<string, long> ErrorCountByCategory { get; init; } = new Dictionary<string, long>();
    public double MessagesPerSecond { get; init; }
}

/// <summary>
/// Settings synchronization status.
/// </summary>
public sealed class SettingsSyncMetrics
{
    public bool IsSynced { get; init; }
    public DateTimeOffset LastRefresh { get; init; }
    public int SettingCount { get; init; }
    public string Version { get; init; } = string.Empty;
}

/// <summary>
/// Shared monitoring snapshot contract returned by /api/monitoring/snapshot.
/// App-specific sections go into AppSpecificMetrics.
/// </summary>
public interface IMonitoringSnapshot
{
    string Application { get; }
    string Instance { get; }
    string Environment { get; }
    DateTimeOffset Timestamp { get; }
    string Version { get; }
    HealthStatus Health { get; }

    KafkaMetrics KafkaMetrics { get; }
    PipelineMetrics PipelineMetrics { get; }
    SettingsSyncMetrics SettingsSyncMetrics { get; }
    IReadOnlyDictionary<string, object> AppSpecificMetrics { get; }
}

/// <summary>
/// Collects and builds a monitoring snapshot for the current app.
/// </summary>
public interface IMonitoringSnapshotCollector
{
    IMonitoringSnapshot Collect();
}
