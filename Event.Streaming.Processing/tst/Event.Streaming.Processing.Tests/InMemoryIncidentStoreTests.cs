using Event.Streaming.Processing.Monitoring;
using Xunit;

namespace Event.Streaming.Processing.Tests;

public sealed class InMemoryIncidentStoreTests
{
    [Fact]
    public void Record_and_GetRecent_returns_most_recent_first()
    {
        var store = new InMemoryIncidentStore();
        store.Record(new OperationalIncident { Message = "first" });
        store.Record(new OperationalIncident { Message = "second" });

        var results = store.GetRecent(10);
        Assert.Equal("second", results[0].Message);
        Assert.Equal("first", results[1].Message);
    }

    [Fact]
    public void GetRecent_honours_count_limit()
    {
        var store = new InMemoryIncidentStore();
        for (int i = 0; i < 10; i++)
            store.Record(new OperationalIncident { Message = $"incident-{i}" });

        var results = store.GetRecent(3);
        Assert.Equal(3, results.Count);
    }

    [Fact]
    public void Rolling_window_evicts_oldest_when_at_capacity()
    {
        const int capacity = 5;
        var store = new InMemoryIncidentStore(capacity);
        for (int i = 0; i < 7; i++)
            store.Record(new OperationalIncident { Message = $"incident-{i}" });

        var all = store.GetRecent(100);
        Assert.Equal(5, all.Count);
        // newest first — incident-6 should be present, incident-0 and incident-1 evicted
        Assert.Equal("incident-6", all[0].Message);
        Assert.DoesNotContain(all, i => i.Message == "incident-0");
        Assert.DoesNotContain(all, i => i.Message == "incident-1");
    }

    [Fact]
    public void GetUnresolved_excludes_resolved_incidents()
    {
        var store = new InMemoryIncidentStore();
        store.Record(new OperationalIncident { Message = "open" });
        store.Record(new OperationalIncident { Message = "closed", IsResolved = true });

        var unresolved = store.GetUnresolved();
        Assert.Single(unresolved);
        Assert.Equal("open", unresolved[0].Message);
    }

    [Fact]
    public void Is_thread_safe_under_concurrent_writes()
    {
        var store = new InMemoryIncidentStore(200);
        var threads = Enumerable.Range(0, 20).Select(_ =>
            new Thread(() =>
            {
                for (int i = 0; i < 20; i++)
                    store.Record(new OperationalIncident { Message = "concurrent" });
            })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        // Should have at most 200 incidents (the capacity)
        Assert.True(store.GetRecent(1000).Count <= 200);
    }
}
