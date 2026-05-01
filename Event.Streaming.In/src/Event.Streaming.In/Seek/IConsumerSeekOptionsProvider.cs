namespace Event.Streaming.In.Seek;

/// <summary>
/// Resolves the effective seek options from configuration / settings snapshot.
/// Apps wire this to their ISettingsSnapshotProvider so seek settings can be
/// changed via SQL without redeployment.
///
/// Settings keys (configurable per-app):
///   {App}:Seek:Mode                      — SeekMode enum name (None/FromOffset/FromTimestamp/Range)
///   {App}:Seek:StartOffsetOrTimestamp    — long offset or ISO 8601 datetime
///   {App}:Seek:StopOffsetOrTimestamp     — (optional) stop boundary
/// </summary>
public interface IConsumerSeekOptionsProvider
{
    ConsumerSeekOptions GetOptions();
}
