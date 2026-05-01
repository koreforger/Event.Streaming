using System.Collections.Concurrent;
using System.Text.Json;
using KoreForge.Jex;
using Newtonsoft.Json.Linq;

namespace Event.Streaming.Processing.Runtime;

public sealed class JexTransformResult
{
    public bool Success { get; init; }
    public JsonElement Payload { get; init; }
    public string? ErrorMessage { get; init; }
}

/// <summary>
/// Applies profile parse expressions using JEX and returns transformed JSON payloads.
/// </summary>
public sealed class JexPayloadTransformer
{
    private readonly IJexCompiler _compiler;
    private readonly ConcurrentDictionary<string, IJexProgram> _cache = new(StringComparer.Ordinal);

    public JexPayloadTransformer(IJexCompiler compiler)
    {
        _compiler = compiler;
    }

    public JexTransformResult Transform(JsonElement payload, string? expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
        {
            return new JexTransformResult { Success = true, Payload = payload.Clone() };
        }

        try
        {
            var program = _cache.GetOrAdd(expression, exp => _compiler.Compile(exp));
            var input = JObject.Parse(payload.GetRawText());
            var result = program.Execute(input);
            var output = result?.ToString(Newtonsoft.Json.Formatting.None) ?? "{}";

            using var doc = JsonDocument.Parse(output);
            return new JexTransformResult
            {
                Success = true,
                Payload = doc.RootElement.Clone(),
            };
        }
        catch (Exception ex)
        {
            return new JexTransformResult
            {
                Success = false,
                Payload = payload.Clone(),
                ErrorMessage = ex.Message,
            };
        }
    }
}
