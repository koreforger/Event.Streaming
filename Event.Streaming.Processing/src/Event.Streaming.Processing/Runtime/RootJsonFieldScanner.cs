using System.Buffers;
using System.Text;

namespace Event.Streaming.Processing.Runtime;

/// <summary>
/// Fast UTF-8 scanner for reading a single root-level JSON string field without full document parsing.
/// </summary>
public sealed class RootJsonFieldScanner
{
    private static readonly byte Quote = (byte)'"';
    private static readonly byte Colon = (byte)':';
    private static readonly byte OpenBrace = (byte)'{';
    private static readonly byte CloseBrace = (byte)'}';
    private static readonly byte OpenBracket = (byte)'[';
    private static readonly byte CloseBracket = (byte)']';
    private static readonly byte Backslash = (byte)'\\';

    public string? ScanRootStringField(ReadOnlySpan<byte> utf8Json, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(fieldName)) return null;

        var keyBytes = Encoding.UTF8.GetBytes($"\"{fieldName}\"");
        var depth = 0;
        var inString = false;
        var i = 0;

        while (i < utf8Json.Length)
        {
            var b = utf8Json[i];

            if (inString)
            {
                if (b == Backslash)
                {
                    i += 2;
                    continue;
                }

                if (b == Quote)
                {
                    inString = false;
                }

                i++;
                continue;
            }

            if (b == OpenBrace || b == OpenBracket)
            {
                depth++;
                i++;
                continue;
            }

            if (b == CloseBrace || b == CloseBracket)
            {
                depth--;
                i++;
                continue;
            }

            if (b == Quote && depth == 1)
            {
                if (i + keyBytes.Length <= utf8Json.Length && utf8Json.Slice(i, keyBytes.Length).SequenceEqual(keyBytes))
                {
                    return ExtractStringValue(utf8Json, i + keyBytes.Length);
                }

                inString = true;
                i++;
                continue;
            }

            i++;
        }

        return null;
    }

    public string? ScanRootStringField(string json, string fieldName)
    {
        var byteCount = Encoding.UTF8.GetByteCount(json);
        byte[]? rented = null;
        Span<byte> buffer = byteCount <= 4096
            ? stackalloc byte[byteCount]
            : (rented = ArrayPool<byte>.Shared.Rent(byteCount)).AsSpan(0, byteCount);

        try
        {
            Encoding.UTF8.GetBytes(json, buffer);
            return ScanRootStringField((ReadOnlySpan<byte>)buffer, fieldName);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    private static string? ExtractStringValue(ReadOnlySpan<byte> utf8Json, int startAfterKey)
    {
        var i = startAfterKey;

        while (i < utf8Json.Length && IsWhitespace(utf8Json[i])) i++;
        if (i >= utf8Json.Length || utf8Json[i] != Colon) return null;

        i++;
        while (i < utf8Json.Length && IsWhitespace(utf8Json[i])) i++;
        if (i >= utf8Json.Length || utf8Json[i] != Quote) return null;

        i++;
        var valueStart = i;

        while (i < utf8Json.Length)
        {
            if (utf8Json[i] == Backslash)
            {
                i += 2;
                continue;
            }

            if (utf8Json[i] == Quote)
            {
                return Encoding.UTF8.GetString(utf8Json.Slice(valueStart, i - valueStart));
            }

            i++;
        }

        return null;
    }

    private static bool IsWhitespace(byte b)
        => b is (byte)' ' or (byte)'\t' or (byte)'\r' or (byte)'\n';
}
