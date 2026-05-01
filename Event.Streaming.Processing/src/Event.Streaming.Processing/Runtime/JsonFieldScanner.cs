using System.Collections.ObjectModel;
using System.Text;
using System.Text.Json;

namespace Event.Streaming.Processing.Runtime;

public sealed class JsonFieldScanner
{
    public JsonFieldScanResult Scan(ReadOnlySpan<byte> utf8Json, IReadOnlyCollection<JsonPathSelector> selectors)
    {
        var selectorByPath = selectors
            .GroupBy(static selector => selector.Path, StringComparer.Ordinal)
            .ToDictionary(static group => group.Key, static group => group.First(), StringComparer.Ordinal);
        var fields = new Dictionary<JsonPathSelector, string?>();
        var frames = new List<JsonFrame>();
        var reader = new Utf8JsonReader(utf8Json);
        var tokensRead = 0;

        try
        {
            while (reader.Read())
            {
                tokensRead++;

                switch (reader.TokenType)
                {
                    case JsonTokenType.PropertyName:
                        SetPendingProperty(frames, reader.GetString());
                        break;
                    case JsonTokenType.StartObject:
                        frames.Add(new JsonFrame(JsonFrameKind.Object, ResolveValuePath(frames)));
                        break;
                    case JsonTokenType.StartArray:
                        frames.Add(new JsonFrame(JsonFrameKind.Array, ResolveValuePath(frames)));
                        break;
                    case JsonTokenType.EndObject:
                    case JsonTokenType.EndArray:
                        if (frames.Count > 0)
                        {
                            frames.RemoveAt(frames.Count - 1);
                        }
                        break;
                    case JsonTokenType.String:
                    case JsonTokenType.Number:
                    case JsonTokenType.True:
                    case JsonTokenType.False:
                    case JsonTokenType.Null:
                        CaptureScalarValue(ref reader, selectorByPath, fields, frames);
                        break;
                }

                if (selectorByPath.Count > 0 && fields.Count == selectorByPath.Count)
                {
                    return JsonFieldScanResult.Success(fields, stoppedEarly: true, tokensRead);
                }
            }

            return JsonFieldScanResult.Success(fields, stoppedEarly: false, tokensRead);
        }
        catch (JsonException ex)
        {
            return JsonFieldScanResult.Invalid(fields, ex.Message, tokensRead);
        }
    }

    private static void CaptureScalarValue(
        ref Utf8JsonReader reader,
        IReadOnlyDictionary<string, JsonPathSelector> selectorByPath,
        Dictionary<JsonPathSelector, string?> fields,
        List<JsonFrame> frames)
    {
        var path = ResolveValuePath(frames);
        if (!selectorByPath.TryGetValue(path, out var selector))
        {
            return;
        }

        fields[selector] = reader.TokenType switch
        {
            JsonTokenType.String => reader.GetString(),
            JsonTokenType.Number => Encoding.UTF8.GetString(reader.ValueSpan),
            JsonTokenType.True => "true",
            JsonTokenType.False => "false",
            JsonTokenType.Null => null,
            _ => null,
        };
    }

    private static string ResolveValuePath(List<JsonFrame> frames)
    {
        if (frames.Count == 0)
        {
            return "$";
        }

        var index = frames.Count - 1;
        var frame = frames[index];
        if (frame.Kind == JsonFrameKind.Object)
        {
            var propertyName = frame.PendingPropertyName
                ?? throw new JsonException("Object value appeared without a property name.");
            frame.PendingPropertyName = null;
            frames[index] = frame;
            return AppendProperty(frame.Path, propertyName);
        }

        var path = $"{frame.Path}[{frame.NextArrayIndex}]";
        frame.NextArrayIndex++;
        frames[index] = frame;
        return path;
    }

    private static void SetPendingProperty(List<JsonFrame> frames, string? propertyName)
    {
        if (frames.Count == 0)
        {
            throw new JsonException("Property name appeared outside an object.");
        }

        var index = frames.Count - 1;
        var frame = frames[index];
        if (frame.Kind != JsonFrameKind.Object)
        {
            throw new JsonException("Property name appeared outside an object.");
        }

        frame.PendingPropertyName = propertyName ?? string.Empty;
        frames[index] = frame;
    }

    private static string AppendProperty(string parentPath, string propertyName) =>
        parentPath == "$" ? $"$.{propertyName}" : $"{parentPath}.{propertyName}";

    private enum JsonFrameKind
    {
        Object,
        Array,
    }

    private struct JsonFrame
    {
        public JsonFrame(JsonFrameKind kind, string path)
        {
            Kind = kind;
            Path = path;
            PendingPropertyName = null;
            NextArrayIndex = 0;
        }

        public JsonFrameKind Kind { get; }
        public string Path { get; }
        public string? PendingPropertyName { get; set; }
        public int NextArrayIndex { get; set; }
    }
}

public sealed record JsonFieldScanResult
{
    private JsonFieldScanResult(
        IReadOnlyDictionary<JsonPathSelector, string?> fields,
        bool isValidJson,
        bool stoppedEarly,
        string? errorMessage,
        int tokensRead)
    {
        Fields = new ReadOnlyDictionary<JsonPathSelector, string?>(new Dictionary<JsonPathSelector, string?>(fields));
        IsValidJson = isValidJson;
        StoppedEarly = stoppedEarly;
        ErrorMessage = errorMessage;
        TokensRead = tokensRead;
    }

    public IReadOnlyDictionary<JsonPathSelector, string?> Fields { get; }
    public bool IsValidJson { get; }
    public bool StoppedEarly { get; }
    public string? ErrorMessage { get; }
    public int TokensRead { get; }

    public string? GetValue(string path) =>
        Fields.TryGetValue(JsonPathSelector.Create(path), out var value) ? value : null;

    public static JsonFieldScanResult Success(
        IReadOnlyDictionary<JsonPathSelector, string?> fields,
        bool stoppedEarly,
        int tokensRead) =>
        new(fields, isValidJson: true, stoppedEarly, errorMessage: null, tokensRead);

    public static JsonFieldScanResult Invalid(
        IReadOnlyDictionary<JsonPathSelector, string?> fields,
        string errorMessage,
        int tokensRead) =>
        new(fields, isValidJson: false, stoppedEarly: false, errorMessage, tokensRead);
}
