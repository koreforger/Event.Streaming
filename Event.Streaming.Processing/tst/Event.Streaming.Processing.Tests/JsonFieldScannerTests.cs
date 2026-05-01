using System.Text;
using Event.Streaming.Processing.Runtime;

namespace Event.Streaming.Processing.Tests;

public sealed class JsonFieldScannerTests
{
    [Fact]
    public void Scan_matches_exact_root_nested_and_array_paths()
    {
        var scanner = new JsonFieldScanner();
        var selectors = new[]
        {
            JsonPathSelector.Create("$.Action"),
            JsonPathSelector.Create("$.Jwt.Action"),
            JsonPathSelector.Create("$.Headers[1].Action"),
            JsonPathSelector.Create("$.NedbankID"),
        };
        var json = """
            {
              "Action": "payment.created",
              "Jwt": { "Action": "jwt.action" },
              "Headers": [
                { "Action": "header-zero" },
                { "Action": "header-one" }
              ],
              "NedbankID": 12345
            }
            """;

        var result = scanner.Scan(Encoding.UTF8.GetBytes(json), selectors);

        Assert.True(result.IsValidJson);
        Assert.Equal("payment.created", result.GetValue("$.Action"));
        Assert.Equal("jwt.action", result.GetValue("$.Jwt.Action"));
        Assert.Equal("header-one", result.GetValue("$.Headers[1].Action"));
        Assert.Equal("12345", result.GetValue("$.NedbankID"));
    }

    [Fact]
    public void Scan_does_not_match_property_name_at_wrong_path()
    {
        var scanner = new JsonFieldScanner();
        var selector = JsonPathSelector.Create("$.Action");
        var json = """{"Jwt":{"Action":"nested-only"}}""";

        var result = scanner.Scan(Encoding.UTF8.GetBytes(json), [selector]);

        Assert.True(result.IsValidJson);
        Assert.False(result.Fields.ContainsKey(selector));
    }

    [Fact]
    public void Scan_stops_after_all_selectors_are_found()
    {
        var scanner = new JsonFieldScanner();
        var selector = JsonPathSelector.Create("$.Action");
        var json = """{"Action":"payment.created","Later":{not valid json}}""";

        var result = scanner.Scan(Encoding.UTF8.GetBytes(json), [selector]);

        Assert.True(result.IsValidJson);
        Assert.True(result.StoppedEarly);
        Assert.Equal("payment.created", result.GetValue("$.Action"));
    }

    [Fact]
    public void Scan_reports_invalid_json_when_required_fields_are_not_found_first()
    {
        var scanner = new JsonFieldScanner();
        var selector = JsonPathSelector.Create("$.Missing");
        var json = """{"Action":"payment.created","Later":{not valid json}}""";

        var result = scanner.Scan(Encoding.UTF8.GetBytes(json), [selector]);

        Assert.False(result.IsValidJson);
        Assert.False(result.StoppedEarly);
        Assert.NotNull(result.ErrorMessage);
    }
}
