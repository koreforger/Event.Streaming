using System.Text.RegularExpressions;

namespace Event.Streaming.Processing.Runtime;

/// <summary>
/// Classifies inbound messages to an active profile using root-field scanning and profile expressions.
/// </summary>
public sealed class ProfileClassifier
{
    private readonly RootJsonFieldScanner _scanner;
    private volatile CompiledProfileSet _compiled = CompiledProfileSet.Empty;

    public ProfileClassifier(RootJsonFieldScanner scanner)
    {
        _scanner = scanner;
    }

    public void Load(IReadOnlyList<ProcessingProfile> profiles)
    {
        var list = new List<CompiledProfile>(profiles.Count);

        foreach (var profile in profiles.Where(p => p.IsActive))
        {
            var expression = profile.ClassificationExpression?.Trim();
            if (string.IsNullOrWhiteSpace(expression))
            {
                list.Add(new CompiledProfile(profile, ClassificationKind.NameEquals, null, profile.Name));
                continue;
            }

            if (expression.StartsWith("regex:", StringComparison.OrdinalIgnoreCase))
            {
                var pattern = expression[6..].Trim();
                if (!string.IsNullOrWhiteSpace(pattern))
                {
                    var regex = new Regex(pattern, RegexOptions.Compiled | RegexOptions.Singleline, TimeSpan.FromMilliseconds(100));
                    list.Add(new CompiledProfile(profile, ClassificationKind.Regex, regex, null));
                    continue;
                }
            }

            if (expression.StartsWith("equals:", StringComparison.OrdinalIgnoreCase))
            {
                list.Add(new CompiledProfile(profile, ClassificationKind.Equals, null, expression[7..].Trim()));
                continue;
            }

            list.Add(new CompiledProfile(profile, ClassificationKind.Equals, null, expression));
        }

        _compiled = new CompiledProfileSet(list);
    }

    public ProcessingProfile? Match(ReadOnlySpan<byte> payloadUtf8)
    {
        var key = _scanner.ScanRootStringField(payloadUtf8, "Action")
            ?? _scanner.ScanRootStringField(payloadUtf8, "eventType");

        if (string.IsNullOrWhiteSpace(key))
        {
            foreach (var profile in _compiled.Profiles)
            {
                if (profile.Kind == ClassificationKind.Regex &&
                    profile.Regex is not null &&
                    profile.Regex.ToString() == ".*")
                {
                    return profile.Profile;
                }
            }

            return null;
        }

        foreach (var profile in _compiled.Profiles)
        {
            switch (profile.Kind)
            {
                case ClassificationKind.NameEquals:
                    if (string.Equals(key, profile.MatchValue, StringComparison.OrdinalIgnoreCase))
                    {
                        return profile.Profile;
                    }
                    break;
                case ClassificationKind.Equals:
                    if (string.Equals(key, profile.MatchValue, StringComparison.OrdinalIgnoreCase))
                    {
                        return profile.Profile;
                    }
                    break;
                case ClassificationKind.Regex:
                    if (profile.Regex is not null && profile.Regex.IsMatch(key))
                    {
                        return profile.Profile;
                    }
                    break;
            }
        }

        return null;
    }

    private enum ClassificationKind
    {
        NameEquals,
        Equals,
        Regex,
    }

    private sealed record CompiledProfile(ProcessingProfile Profile, ClassificationKind Kind, Regex? Regex, string? MatchValue);

    private sealed class CompiledProfileSet
    {
        public static readonly CompiledProfileSet Empty = new([]);
        public IReadOnlyList<CompiledProfile> Profiles { get; }

        public CompiledProfileSet(IReadOnlyList<CompiledProfile> profiles)
        {
            Profiles = profiles;
        }
    }
}
