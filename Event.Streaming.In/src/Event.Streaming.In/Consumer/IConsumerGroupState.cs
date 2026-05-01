namespace Event.Streaming.In.Consumer;

/// <summary>
/// Consumer group membership lifecycle states.
/// </summary>
public enum ConsumerMemberState
{
    Initializing,
    Assigning,
    Running,
    Rebalancing,
    Paused,
    Stopping,
    Stopped,
    Faulted,
}

/// <summary>
/// Partition assignment with current and end-of-log offset details.
/// </summary>
public sealed class TopicPartitionAssignment
{
    public string Topic { get; init; } = string.Empty;
    public int Partition { get; init; }
    public long CurrentOffset { get; init; }
    public long LogEndOffset { get; init; }
    public long Lag => LogEndOffset > CurrentOffset ? LogEndOffset - CurrentOffset : 0;
}

/// <summary>
/// Consumer group state snapshot: group membership, assignments, and state.
/// </summary>
public interface IConsumerGroupState
{
    string GroupId { get; }
    ConsumerMemberState State { get; }
    IReadOnlyList<TopicPartitionAssignment> Assignments { get; }
    DateTimeOffset StateChangeTime { get; }
    int RebalanceCount { get; }
    DateTimeOffset? LastRebalanceTime { get; }
}

/// <summary>
/// Consumer health metrics: lag, liveness, and last-message time.
/// </summary>
public interface IConsumerHealthMetrics
{
    long TotalLag { get; }
    int AssignedPartitions { get; }
    bool IsHealthy { get; }
    DateTimeOffset LastMessageTime { get; }
    IReadOnlyDictionary<int, long> LagPerPartition { get; }
}
