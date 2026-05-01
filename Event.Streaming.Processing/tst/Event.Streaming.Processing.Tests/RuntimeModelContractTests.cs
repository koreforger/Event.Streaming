using Event.Streaming.Processing.Runtime;

namespace Event.Streaming.Processing.Tests;

public sealed class RuntimeModelContractTests
{
    [Fact]
    public void Runtime_model_copies_collections_when_published()
    {
        var sourceSystems = new Dictionary<string, SourceSystemDefinition>
        {
            ["provider-a"] = new SourceSystemDefinition(
                "provider-a",
                "ProviderA.Payments",
                "Provider A",
                true,
                new Dictionary<string, string> { ["region"] = "za" }),
        };
        var discriminatorPaths = new List<JsonPathSelector> { JsonPathSelector.Create("$.Action") };
        var clientPaths = new List<JsonPathSelector> { JsonPathSelector.Create("$.NedbankID") };
        var functions = new Dictionary<int, CompiledFunctionPlan>
        {
            [123] = CreateFunctionPlan(123, functionVersion: 7, scriptVersion: 11, ruleSetVersion: 13, routeVersion: 17),
        };

        var model = new EventReaderRuntimeModel(
            42,
            DateTimeOffset.Parse("2026-04-28T10:00:00Z"),
            sourceSystems,
            discriminatorPaths,
            clientPaths,
            FunctionMatcherIndex.Empty,
            functions);

        sourceSystems.Clear();
        discriminatorPaths.Clear();
        clientPaths.Clear();
        functions.Clear();

        Assert.Single(model.SourceSystems);
        Assert.Single(model.FunctionDiscriminatorPaths);
        Assert.Single(model.ClientIdentityPaths);
        Assert.Single(model.Functions);
    }

    [Fact]
    public void Resolve_function_plan_uses_global_plan_when_source_has_no_override()
    {
        var function = CreateFunctionPlan(123, functionVersion: 7, scriptVersion: 11, ruleSetVersion: 13, routeVersion: 17);
        var model = CreateRuntimeModel(function);

        var resolved = model.ResolveFunctionPlan(123, "provider-a");

        Assert.NotNull(resolved);
        Assert.Equal(42, resolved.RuntimeModelVersion);
        Assert.Equal(123, resolved.FunctionId);
        Assert.Equal(7, resolved.FunctionVersion);
        Assert.Equal(11, resolved.ExtractionScriptVersion);
        Assert.Equal(13, resolved.RuleSetVersion);
        Assert.Equal(17, resolved.OutputRouteVersion);
        Assert.Equal("global-out", resolved.OutputRoute.RouteName);
    }

    [Fact]
    public void Resolve_function_plan_uses_source_specific_plan_when_override_exists()
    {
        var sourceOverride = new SourceSystemFunctionPlan(
            new CompiledJexScript("provider-b-extract", 21),
            new CompiledRuleSet(23, []),
            new OutputRoutePlan(29, "provider-b-out", "provider-b.output"),
            FunctionFailurePolicy.Default);
        var function = CreateFunctionPlan(
            123,
            functionVersion: 7,
            scriptVersion: 11,
            ruleSetVersion: 13,
            routeVersion: 17,
            new SourceSystemBehavior(new Dictionary<string, SourceSystemFunctionPlan>
            {
                ["provider-b"] = sourceOverride,
            }));
        var model = CreateRuntimeModel(function);

        var resolved = model.ResolveFunctionPlan(123, "provider-b");

        Assert.NotNull(resolved);
        Assert.Equal(42, resolved.RuntimeModelVersion);
        Assert.Equal(123, resolved.FunctionId);
        Assert.Equal(7, resolved.FunctionVersion);
        Assert.Equal(21, resolved.ExtractionScriptVersion);
        Assert.Equal(23, resolved.RuleSetVersion);
        Assert.Equal(29, resolved.OutputRouteVersion);
        Assert.Equal("provider-b-out", resolved.OutputRoute.RouteName);
    }

    [Fact]
    public void Json_path_selector_requires_absolute_json_path()
    {
        var exception = Assert.Throws<ArgumentException>(() => JsonPathSelector.Create("Action"));

        Assert.Contains("absolute JSON path", exception.Message);
    }

    private static EventReaderRuntimeModel CreateRuntimeModel(CompiledFunctionPlan function) =>
        new(
            42,
            DateTimeOffset.Parse("2026-04-28T10:00:00Z"),
            new Dictionary<string, SourceSystemDefinition>
            {
                ["provider-a"] = new SourceSystemDefinition("provider-a", "ProviderA.Payments", "Provider A", true, new Dictionary<string, string>()),
                ["provider-b"] = new SourceSystemDefinition("provider-b", "ProviderB.Payments", "Provider B", true, new Dictionary<string, string>()),
            },
            [JsonPathSelector.Create("$.Action")],
            [JsonPathSelector.Create("$.NedbankID")],
            FunctionMatcherIndex.Empty,
            new Dictionary<int, CompiledFunctionPlan> { [function.FunctionId] = function });

    private static CompiledFunctionPlan CreateFunctionPlan(
        int functionId,
        long functionVersion,
        long scriptVersion,
        long ruleSetVersion,
        long routeVersion,
        SourceSystemBehavior? sourceSystemBehavior = null) =>
        new(
            functionId,
            functionVersion,
            "payments",
            sourceSystemBehavior ?? SourceSystemBehavior.Global,
            new CompiledJexScript("global-extract", scriptVersion),
            new CompiledRuleSet(ruleSetVersion, []),
            new OutputRoutePlan(routeVersion, "global-out", "global.output"),
            FunctionFailurePolicy.Default);
}
