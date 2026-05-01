namespace Event.Streaming.Processing.Runtime;

/// <summary>
/// Shared profile definition used by stream readers to classify, parse, and route inbound messages.
/// </summary>
public sealed class ProcessingProfile
{
    public string Name { get; init; } = string.Empty;
    public string? Description { get; init; }
    public bool IsActive { get; init; } = true;
    public string? ClassificationExpression { get; init; }
    public string? ParseExpression { get; init; }
    public IReadOnlyDictionary<string, string> RoutingTagOverrides { get; init; }
        = new Dictionary<string, string>();
    public IReadOnlyList<ProcessingOutputRoute> OutputRoutes { get; init; }
        = Array.Empty<ProcessingOutputRoute>();
}

public sealed class ProcessingOutputRoute
{
    public string RouteName { get; init; } = string.Empty;
    public string TargetTopic { get; init; } = string.Empty;
    public bool IsRequired { get; init; } = true;
}

public interface IProcessingProfileCatalog
{
    IReadOnlyList<ProcessingProfile> GetActiveProfiles();
}
