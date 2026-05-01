using System.Text.RegularExpressions;
using Event.Streaming.Processing.Runtime;

namespace Event.Streaming.Processing.Tests;

public sealed class FunctionMatcherIndexTests
{
    [Fact]
    public void Match_returns_null_when_prefix_bucket_misses()
    {
        var index = new FunctionMatcherIndex(
            [
                Matcher(null, functionId: 100, priority: 1, prefix: "transfer.", pattern: "^payment\\.created$")
            ],
            prefixLength: 9);

        var match = index.Match("provider-a", "payment.created");

        Assert.Null(match);
    }

    [Fact]
    public void Match_uses_priority_then_function_id_for_deterministic_ordering()
    {
        var index = new FunctionMatcherIndex(
            [
                Matcher(null, functionId: 300, priority: 2, prefix: "payment.c", pattern: "^payment\\.created$"),
                Matcher(null, functionId: 200, priority: 1, prefix: "payment.c", pattern: "^payment\\.created$"),
                Matcher(null, functionId: 100, priority: 1, prefix: "payment.c", pattern: "^payment\\.created$")
            ],
            prefixLength: 9);

        var match = index.Match("provider-a", "payment.created");

        Assert.NotNull(match);
        Assert.Equal(100, match.FunctionId);
    }

    [Fact]
    public void Match_prefers_source_specific_matcher_over_global_matcher()
    {
        var index = new FunctionMatcherIndex(
            [
                Matcher(null, functionId: 100, priority: 1, prefix: "payment.c", pattern: "^payment\\.created$"),
                Matcher("provider-b", functionId: 200, priority: 10, prefix: "payment.c", pattern: "^payment\\.created$")
            ],
            prefixLength: 9);

        var match = index.Match("provider-b", "payment.created");

        Assert.NotNull(match);
        Assert.Equal(200, match.FunctionId);
    }

    [Fact]
    public void Match_falls_back_to_global_matcher_when_source_specific_does_not_match()
    {
        var index = new FunctionMatcherIndex(
            [
                Matcher("provider-b", functionId: 200, priority: 1, prefix: "payment.c", pattern: "^payment\\.reversed$"),
                Matcher(null, functionId: 100, priority: 1, prefix: "payment.c", pattern: "^payment\\.created$")
            ],
            prefixLength: 9);

        var match = index.Match("provider-b", "payment.created");

        Assert.NotNull(match);
        Assert.Equal(100, match.FunctionId);
    }

    private static FunctionMatcherDefinition Matcher(
        string? sourceSystemId,
        int functionId,
        int priority,
        string prefix,
        string pattern) =>
        new(
            sourceSystemId,
            functionId,
            priority,
            prefix,
            new Regex(pattern, RegexOptions.Compiled | RegexOptions.CultureInvariant));
}
