using System.Collections.ObjectModel;
using System.Text.RegularExpressions;

namespace Event.Streaming.Processing.Runtime;

public sealed record EventReaderRuntimeModel
{
    public EventReaderRuntimeModel(
        long version,
        DateTimeOffset createdUtc,
        IReadOnlyDictionary<string, SourceSystemDefinition> sourceSystems,
        IReadOnlyList<JsonPathSelector> functionDiscriminatorPaths,
        IReadOnlyList<JsonPathSelector> clientIdentityPaths,
        FunctionMatcherIndex functionMatchers,
        IReadOnlyDictionary<int, CompiledFunctionPlan> functions)
    {
        Version = version;
        CreatedUtc = createdUtc;
        SourceSystems = CopyStringDictionary(sourceSystems, StringComparer.OrdinalIgnoreCase);
        FunctionDiscriminatorPaths = functionDiscriminatorPaths.ToArray();
        ClientIdentityPaths = clientIdentityPaths.ToArray();
        FunctionMatchers = functionMatchers;
        Functions = new ReadOnlyDictionary<int, CompiledFunctionPlan>(new Dictionary<int, CompiledFunctionPlan>(functions));
    }

    public long Version { get; }
    public DateTimeOffset CreatedUtc { get; }
    public IReadOnlyDictionary<string, SourceSystemDefinition> SourceSystems { get; }
    public IReadOnlyList<JsonPathSelector> FunctionDiscriminatorPaths { get; }
    public IReadOnlyList<JsonPathSelector> ClientIdentityPaths { get; }
    public FunctionMatcherIndex FunctionMatchers { get; }
    public IReadOnlyDictionary<int, CompiledFunctionPlan> Functions { get; }

    public ResolvedFunctionPlan? ResolveFunctionPlan(int functionId, string sourceSystemId)
    {
        if (!Functions.TryGetValue(functionId, out var function))
        {
            return null;
        }

        var sourcePlan = function.SourceSystemBehavior.ResolveOverride(sourceSystemId);
        return sourcePlan is null
            ? ResolvedFunctionPlan.FromGlobal(Version, function)
            : ResolvedFunctionPlan.FromOverride(Version, function, sourcePlan);
    }

    public string? ResolveSourceSystemId(string kafkaTopic)
    {
        foreach (var kvp in SourceSystems)
        {
            if (string.Equals(kvp.Value.KafkaTopic, kafkaTopic, StringComparison.OrdinalIgnoreCase))
            {
                return kvp.Key;
            }
        }

        return null;
    }

    internal static IReadOnlyDictionary<string, TValue> CopyStringDictionary<TValue>(
        IReadOnlyDictionary<string, TValue> source,
        StringComparer comparer) =>
        new ReadOnlyDictionary<string, TValue>(new Dictionary<string, TValue>(source, comparer));
}

public sealed record SourceSystemDefinition
{
    public SourceSystemDefinition(
        string sourceSystemId,
        string kafkaTopic,
        string providerName,
        bool isEnabled,
        IReadOnlyDictionary<string, string> tags)
    {
        SourceSystemId = sourceSystemId;
        KafkaTopic = kafkaTopic;
        ProviderName = providerName;
        IsEnabled = isEnabled;
        Tags = EventReaderRuntimeModel.CopyStringDictionary(tags, StringComparer.OrdinalIgnoreCase);
    }

    public string SourceSystemId { get; }
    public string KafkaTopic { get; }
    public string ProviderName { get; }
    public bool IsEnabled { get; }
    public IReadOnlyDictionary<string, string> Tags { get; }
}

public sealed record JsonPathSelector
{
    private JsonPathSelector(string path)
    {
        Path = path;
    }

    public string Path { get; }

    public static JsonPathSelector Create(string path)
    {
        if (string.IsNullOrWhiteSpace(path) || !path.StartsWith('$'))
        {
            throw new ArgumentException("JsonPathSelector requires an absolute JSON path beginning with '$'.", nameof(path));
        }

        return new JsonPathSelector(path);
    }

    public override string ToString() => Path;
}

public sealed record CompiledFunctionPlan(
    int FunctionId,
    long FunctionVersion,
    string Name,
    SourceSystemBehavior SourceSystemBehavior,
    CompiledJexScript ExtractionScript,
    CompiledRuleSet RuleSet,
    OutputRoutePlan OutputRoute,
    FunctionFailurePolicy FailurePolicy);

public sealed record SourceSystemBehavior
{
    public static SourceSystemBehavior Global { get; } = new(new Dictionary<string, SourceSystemFunctionPlan>());

    public SourceSystemBehavior(IReadOnlyDictionary<string, SourceSystemFunctionPlan> sourceSystemOverrides)
    {
        SourceSystemOverrides = EventReaderRuntimeModel.CopyStringDictionary(sourceSystemOverrides, StringComparer.OrdinalIgnoreCase);
    }

    public IReadOnlyDictionary<string, SourceSystemFunctionPlan> SourceSystemOverrides { get; }

    public SourceSystemFunctionPlan? ResolveOverride(string sourceSystemId) =>
        SourceSystemOverrides.TryGetValue(sourceSystemId, out var plan) ? plan : null;
}

public sealed record SourceSystemFunctionPlan(
    CompiledJexScript ExtractionScript,
    CompiledRuleSet RuleSet,
    OutputRoutePlan OutputRoute,
    FunctionFailurePolicy FailurePolicy);

public sealed record CompiledJexScript(string Name, long Version);

public sealed record CompiledRuleSet(long Version, IReadOnlyList<CompiledRule> Rules)
{
    public IReadOnlyList<CompiledRule> Rules { get; init; } = Rules.ToArray();
}

public sealed record CompiledRule(int RuleId, long RuleVersion, string Name);

public sealed record OutputRoutePlan(long Version, string RouteName, string Topic);

public sealed record FunctionFailurePolicy
{
    public static FunctionFailurePolicy Default { get; } = new();

    public bool FailWhenNedbankIdMissing { get; init; }
    public int MaxProcessingAttempts { get; init; } = 3;
    public int MaxOutputAttempts { get; init; } = 3;
}

public sealed record ResolvedFunctionPlan(
    long RuntimeModelVersion,
    int FunctionId,
    long FunctionVersion,
    string Name,
    CompiledJexScript ExtractionScript,
    CompiledRuleSet RuleSet,
    OutputRoutePlan OutputRoute,
    FunctionFailurePolicy FailurePolicy)
{
    public long ExtractionScriptVersion => ExtractionScript.Version;
    public long RuleSetVersion => RuleSet.Version;
    public long OutputRouteVersion => OutputRoute.Version;

    public static ResolvedFunctionPlan FromGlobal(long runtimeModelVersion, CompiledFunctionPlan function) =>
        new(
            runtimeModelVersion,
            function.FunctionId,
            function.FunctionVersion,
            function.Name,
            function.ExtractionScript,
            function.RuleSet,
            function.OutputRoute,
            function.FailurePolicy);

    public static ResolvedFunctionPlan FromOverride(
        long runtimeModelVersion,
        CompiledFunctionPlan function,
        SourceSystemFunctionPlan sourcePlan) =>
        new(
            runtimeModelVersion,
            function.FunctionId,
            function.FunctionVersion,
            function.Name,
            sourcePlan.ExtractionScript,
            sourcePlan.RuleSet,
            sourcePlan.OutputRoute,
            sourcePlan.FailurePolicy);
}

public sealed record FunctionMatcherDefinition(
    string? SourceSystemId,
    int FunctionId,
    int Priority,
    string Prefix,
    Regex Regex);

public sealed record FunctionMatcherIndex
{
    public static FunctionMatcherIndex Empty { get; } = new([], 10);
    private readonly IReadOnlyDictionary<MatcherBucketKey, IReadOnlyList<FunctionMatcherDefinition>> _buckets;

    public FunctionMatcherIndex(IReadOnlyList<FunctionMatcherDefinition> matchers, int prefixLength)
    {
        if (prefixLength <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(prefixLength), "Prefix length must be positive.");
        }

        PrefixLength = prefixLength;
        Matchers = matchers.ToArray();
        _buckets = BuildBuckets(Matchers, PrefixLength);
    }

    public IReadOnlyList<FunctionMatcherDefinition> Matchers { get; }
    public int PrefixLength { get; }

    public FunctionMatchResult? Match(string sourceSystemId, string discriminatorValue)
    {
        if (string.IsNullOrWhiteSpace(discriminatorValue))
        {
            return null;
        }

        var normalizedValue = Normalize(discriminatorValue);
        var prefix = GetPrefix(normalizedValue, PrefixLength);
        var normalizedSource = NormalizeSource(sourceSystemId);

        return MatchBucket(new MatcherBucketKey(normalizedSource, prefix), normalizedValue)
            ?? MatchBucket(new MatcherBucketKey(null, prefix), normalizedValue);
    }

    private FunctionMatchResult? MatchBucket(MatcherBucketKey key, string normalizedValue)
    {
        if (!_buckets.TryGetValue(key, out var candidates))
        {
            return null;
        }

        foreach (var candidate in candidates)
        {
            if (candidate.Regex.IsMatch(normalizedValue))
            {
                return new FunctionMatchResult(
                    candidate.FunctionId,
                    candidate.SourceSystemId,
                    candidate.Priority,
                    candidate);
            }
        }

        return null;
    }

    private static IReadOnlyDictionary<MatcherBucketKey, IReadOnlyList<FunctionMatcherDefinition>> BuildBuckets(
        IReadOnlyList<FunctionMatcherDefinition> matchers,
        int prefixLength)
    {
        return matchers
            .GroupBy(
                matcher => new MatcherBucketKey(
                    NormalizeSource(matcher.SourceSystemId),
                    GetPrefix(Normalize(matcher.Prefix), prefixLength)))
            .ToDictionary(
                group => group.Key,
                group => (IReadOnlyList<FunctionMatcherDefinition>)group
                    .OrderBy(static matcher => matcher.Priority)
                    .ThenBy(static matcher => matcher.FunctionId)
                    .ToArray());
    }

    private static string Normalize(string value) => value.Trim().ToLowerInvariant();

    private static string? NormalizeSource(string? sourceSystemId) =>
        string.IsNullOrWhiteSpace(sourceSystemId) ? null : Normalize(sourceSystemId);

    private static string GetPrefix(string normalizedValue, int prefixLength) =>
        normalizedValue.Length <= prefixLength ? normalizedValue : normalizedValue[..prefixLength];

    private readonly record struct MatcherBucketKey(string? SourceSystemId, string Prefix);
}

public sealed record FunctionMatchResult(
    int FunctionId,
    string? SourceSystemId,
    int Priority,
    FunctionMatcherDefinition Matcher);
