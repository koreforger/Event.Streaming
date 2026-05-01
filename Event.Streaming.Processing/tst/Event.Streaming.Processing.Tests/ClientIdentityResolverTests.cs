using System.Text;
using Event.Streaming.Processing.Runtime;

namespace Event.Streaming.Processing.Tests;

public sealed class ClientIdentityResolverTests
{
    [Fact]
    public async Task Resolve_prefers_nedbank_id_over_username()
    {
        var lookup = new RecordingUsernameLookup(_ => Task.FromResult<long?>(999));
        var resolver = new ClientIdentityResolver(lookup);
        var scan = Scan("""{"NedbankID":123,"NedbankIDUsername":"user-a"}""");

        var result = await resolver.ResolveAsync(scan, CancellationToken.None);

        Assert.Equal(123, result.NedbankId);
        Assert.Equal(ClientIdentitySource.NedbankId, result.Source);
        Assert.Equal(0, lookup.CallCount);
    }

    [Fact]
    public async Task Resolve_caches_positive_username_lookup()
    {
        var lookup = new RecordingUsernameLookup(_ => Task.FromResult<long?>(456));
        var resolver = new ClientIdentityResolver(lookup);
        var scan = Scan("""{"NedbankIDUsername":"user-a"}""");

        var first = await resolver.ResolveAsync(scan, CancellationToken.None);
        var second = await resolver.ResolveAsync(scan, CancellationToken.None);

        Assert.Equal(456, first.NedbankId);
        Assert.Equal(456, second.NedbankId);
        Assert.Equal(1, lookup.CallCount);
    }

    [Fact]
    public async Task Resolve_caches_negative_username_lookup()
    {
        var lookup = new RecordingUsernameLookup(_ => Task.FromResult<long?>(null));
        var resolver = new ClientIdentityResolver(lookup);
        var scan = Scan("""{"NedbankIDUsername":"unknown-user"}""");

        var first = await resolver.ResolveAsync(scan, CancellationToken.None);
        var second = await resolver.ResolveAsync(scan, CancellationToken.None);

        Assert.Equal(0, first.NedbankId);
        Assert.Equal(0, second.NedbankId);
        Assert.Equal(ClientIdentityLookupStatus.NotFound, first.LookupStatus);
        Assert.Equal(1, lookup.CallCount);
    }

    [Fact]
    public async Task Resolve_returns_zero_when_identity_is_missing()
    {
        var resolver = new ClientIdentityResolver(new RecordingUsernameLookup(_ => Task.FromResult<long?>(999)));
        var scan = Scan("""{"Action":"payment.created"}""");

        var result = await resolver.ResolveAsync(scan, CancellationToken.None);

        Assert.Equal(0, result.NedbankId);
        Assert.Equal(ClientIdentitySource.None, result.Source);
    }

    [Fact]
    public async Task Resolve_returns_zero_when_username_lookup_times_out()
    {
        var resolver = new ClientIdentityResolver(
            new RecordingUsernameLookup(async _ =>
            {
                await Task.Delay(TimeSpan.FromSeconds(10));
                return 999;
            }),
            new ClientIdentityResolverOptions
            {
                LookupTimeout = TimeSpan.FromMilliseconds(25),
                MaxConcurrentLookups = 1,
            });
        var scan = Scan("""{"NedbankIDUsername":"slow-user"}""");

        var result = await resolver.ResolveAsync(scan, CancellationToken.None);

        Assert.Equal(0, result.NedbankId);
        Assert.Equal(ClientIdentityLookupStatus.TimedOut, result.LookupStatus);
    }

    [Fact]
    public async Task Resolve_bounds_concurrent_username_lookups()
    {
        var lookup = new ConcurrentTrackingUsernameLookup();
        var resolver = new ClientIdentityResolver(
            lookup,
            new ClientIdentityResolverOptions
            {
                LookupTimeout = TimeSpan.FromSeconds(5),
                MaxConcurrentLookups = 2,
            });

        var tasks = Enumerable.Range(0, 5)
            .Select(i => resolver.ResolveAsync(Scan($$"""{"NedbankIDUsername":"user-{{i}}"}"""), CancellationToken.None))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        Assert.All(results, result => Assert.Equal(1000, result.NedbankId));
        Assert.True(lookup.MaxActiveLookups <= 2);
    }

    private static JsonFieldScanResult Scan(string json)
    {
        var scanner = new JsonFieldScanner();
        return scanner.Scan(
            Encoding.UTF8.GetBytes(json),
            [JsonPathSelector.Create("$.NedbankID"), JsonPathSelector.Create("$.NedbankIDUsername")]);
    }

    private sealed class RecordingUsernameLookup : IUsernameIdentityLookup
    {
        private readonly Func<string, Task<long?>> _resolve;
        private int _callCount;

        public RecordingUsernameLookup(Func<string, Task<long?>> resolve)
        {
            _resolve = resolve;
        }

        public int CallCount => Volatile.Read(ref _callCount);

        public async Task<long?> ResolveNedbankIdAsync(string username, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _callCount);
            return await _resolve(username).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class ConcurrentTrackingUsernameLookup : IUsernameIdentityLookup
    {
        private int _activeLookups;
        private int _maxActiveLookups;

        public int MaxActiveLookups => Volatile.Read(ref _maxActiveLookups);

        public async Task<long?> ResolveNedbankIdAsync(string username, CancellationToken cancellationToken)
        {
            var active = Interlocked.Increment(ref _activeLookups);
            UpdateMax(active);

            try
            {
                await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                return 1000;
            }
            finally
            {
                Interlocked.Decrement(ref _activeLookups);
            }
        }

        private void UpdateMax(int active)
        {
            while (true)
            {
                var current = Volatile.Read(ref _maxActiveLookups);
                if (active <= current ||
                    Interlocked.CompareExchange(ref _maxActiveLookups, active, current) == current)
                {
                    return;
                }
            }
        }
    }
}
