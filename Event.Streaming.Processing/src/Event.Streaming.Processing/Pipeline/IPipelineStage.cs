namespace Event.Streaming.Processing.Pipeline;

/// <summary>
/// Result returned by a pipeline stage execution.
/// </summary>
public sealed class StageExecutionResult<T>
{
    public bool Success { get; init; }
    public T? Output { get; init; }
    public TimeSpan ExecutionTime { get; init; }
    public string? ErrorCategory { get; init; }
    public string? ErrorMessage { get; init; }

    public static StageExecutionResult<T> Ok(T output, TimeSpan elapsed) =>
        new() { Success = true, Output = output, ExecutionTime = elapsed };

    public static StageExecutionResult<T> Fail(string category, string message, TimeSpan elapsed) =>
        new() { Success = false, ErrorCategory = category, ErrorMessage = message, ExecutionTime = elapsed };
}

/// <summary>
/// A single named stage in the processing pipeline.
/// </summary>
public interface IPipelineStage<TInput, TOutput>
{
    string StageName { get; }
    Task<StageExecutionResult<TOutput>> ExecuteAsync(TInput input, CancellationToken ct);
}
