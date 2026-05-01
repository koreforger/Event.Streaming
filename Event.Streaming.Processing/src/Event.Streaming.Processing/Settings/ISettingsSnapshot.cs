namespace Event.Streaming.Processing.Settings;

/// <summary>
/// Immutable snapshot of resolved settings consumed by runtime services.
/// Services should react to SnapshotChanged rather than querying settings on every call.
/// </summary>
public interface ISettingsSnapshot
{
    string? GetValue(string key);
    T? GetValue<T>(string key) where T : struct;
    string GetRequiredValue(string key);
    DateTimeOffset SnapshotTime { get; }
    string Version { get; }
    int Count { get; }
}

/// <summary>
/// Provides the current settings snapshot and notifies subscribers when it changes.
/// </summary>
public interface ISettingsSnapshotProvider
{
    ISettingsSnapshot Current { get; }
    event Action<ISettingsSnapshot>? SnapshotChanged;
}
