using System.Collections.Concurrent;

namespace Event.Streaming.Processing.Runtime;

public interface IUsernameIdentityLookup
{
    Task<long?> ResolveNedbankIdAsync(string username, CancellationToken cancellationToken);
}

public sealed class ClientIdentityResolver
{
    private const string NedbankIdPath = "$.NedbankID";
    private const string NedbankUsernamePath = "$.NedbankIDUsername";

    private readonly IUsernameIdentityLookup _usernameLookup;
    private readonly ClientIdentityResolverOptions _options;
    private readonly ConcurrentDictionary<string, long> _positiveCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, byte> _negativeCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _lookupGate;

    public ClientIdentityResolver(IUsernameIdentityLookup usernameLookup)
        : this(usernameLookup, new ClientIdentityResolverOptions())
    {
    }

    public ClientIdentityResolver(IUsernameIdentityLookup usernameLookup, ClientIdentityResolverOptions options)
    {
        if (options.MaxConcurrentLookups <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Max concurrent username lookups must be positive.");
        }

        _usernameLookup = usernameLookup;
        _options = options;
        _lookupGate = new SemaphoreSlim(options.MaxConcurrentLookups, options.MaxConcurrentLookups);
    }

    public async Task<ClientIdentityResolution> ResolveAsync(
        JsonFieldScanResult scanResult,
        CancellationToken cancellationToken)
    {
        var nedbankIdValue = scanResult.GetValue(NedbankIdPath);
        if (long.TryParse(nedbankIdValue, out var nedbankId))
        {
            return new ClientIdentityResolution(
                nedbankId,
                ClientIdentitySource.NedbankId,
                ClientIdentityLookupStatus.NotRequired);
        }

        var username = scanResult.GetValue(NedbankUsernamePath);
        if (string.IsNullOrWhiteSpace(username))
        {
            return ClientIdentityResolution.Missing;
        }

        if (_positiveCache.TryGetValue(username, out var cachedNedbankId))
        {
            return new ClientIdentityResolution(
                cachedNedbankId,
                ClientIdentitySource.Username,
                ClientIdentityLookupStatus.CacheHit);
        }

        if (_negativeCache.ContainsKey(username))
        {
            return new ClientIdentityResolution(
                0,
                ClientIdentitySource.Username,
                ClientIdentityLookupStatus.NotFound);
        }

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_options.LookupTimeout);

        try
        {
            await _lookupGate.WaitAsync(timeoutCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return new ClientIdentityResolution(
                0,
                ClientIdentitySource.Username,
                ClientIdentityLookupStatus.TimedOut);
        }

        try
        {
            var resolved = await _usernameLookup
                .ResolveNedbankIdAsync(username, timeoutCts.Token)
                .ConfigureAwait(false);

            if (resolved is long resolvedNedbankId)
            {
                _positiveCache[username] = resolvedNedbankId;
                return new ClientIdentityResolution(
                    resolvedNedbankId,
                    ClientIdentitySource.Username,
                    ClientIdentityLookupStatus.Found);
            }

            _negativeCache[username] = 0;
            return new ClientIdentityResolution(
                0,
                ClientIdentitySource.Username,
                ClientIdentityLookupStatus.NotFound);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return new ClientIdentityResolution(
                0,
                ClientIdentitySource.Username,
                ClientIdentityLookupStatus.TimedOut);
        }
        catch
        {
            return new ClientIdentityResolution(
                0,
                ClientIdentitySource.Username,
                ClientIdentityLookupStatus.Failed);
        }
        finally
        {
            _lookupGate.Release();
        }
    }
}

public sealed record ClientIdentityResolverOptions
{
    public TimeSpan LookupTimeout { get; init; } = TimeSpan.FromMilliseconds(250);
    public int MaxConcurrentLookups { get; init; } = 8;
}

public sealed record ClientIdentityResolution(
    long NedbankId,
    ClientIdentitySource Source,
    ClientIdentityLookupStatus LookupStatus)
{
    public static ClientIdentityResolution Missing { get; } = new(
        0,
        ClientIdentitySource.None,
        ClientIdentityLookupStatus.Missing);
}

public enum ClientIdentitySource
{
    None,
    NedbankId,
    Username,
}

public enum ClientIdentityLookupStatus
{
    Missing,
    NotRequired,
    CacheHit,
    Found,
    NotFound,
    TimedOut,
    Failed,
}
