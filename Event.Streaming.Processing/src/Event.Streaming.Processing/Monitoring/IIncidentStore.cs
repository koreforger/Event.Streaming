namespace Event.Streaming.Processing.Monitoring;

/// <summary>
/// Operational incident (rebalance, DLQ push, pipeline error, etc.).
/// </summary>
public sealed class OperationalIncident
{
    public Guid IncidentId { get; init; } = Guid.NewGuid();
    public string Application { get; init; } = string.Empty;
    public string InstanceId { get; init; } = string.Empty;
    public string Category { get; init; } = string.Empty;
    public string Severity { get; init; } = "Warning";
    public string Message { get; init; } = string.Empty;
    public string? Detail { get; init; }
    public DateTimeOffset OccurredAt { get; init; } = DateTimeOffset.UtcNow;
    public bool IsResolved { get; set; }
    public DateTimeOffset? ResolvedAt { get; set; }
}

/// <summary>
/// In-process incident store. Holds a rolling window of recent incidents in memory.
/// </summary>
public interface IIncidentStore
{
    void Record(OperationalIncident incident);
    IReadOnlyList<OperationalIncident> GetRecent(int count = 50);
    IReadOnlyList<OperationalIncident> GetUnresolved();
}
