namespace Event.Streaming.Processing.WorkStore;

public interface IWorkItemProcessor
{
    Task<WorkItemProcessingResult> ProcessAsync(WorkLease lease, CancellationToken ct);
}

public sealed record WorkItemProcessingResult
{
    public byte[]? OutputPayload { get; init; }
    public bool Success { get; init; }
    public string? ErrorMessage { get; init; }
    public bool ShouldRetry { get; init; }
}
