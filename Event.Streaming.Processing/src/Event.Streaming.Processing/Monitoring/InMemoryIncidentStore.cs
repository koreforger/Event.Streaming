namespace Event.Streaming.Processing.Monitoring;

/// <summary>
/// Thread-safe in-memory rolling incident store.
/// Keeps the most recent N incidents (configured via constructor).
/// </summary>
public sealed class InMemoryIncidentStore : IIncidentStore
{
    private readonly int _capacity;
    private readonly LinkedList<OperationalIncident> _incidents = new();
    private readonly Lock _lock = new();

    public InMemoryIncidentStore(int capacity = 200) => _capacity = capacity;

    public void Record(OperationalIncident incident)
    {
        lock (_lock)
        {
            _incidents.AddFirst(incident);
            while (_incidents.Count > _capacity)
                _incidents.RemoveLast();
        }
    }

    public IReadOnlyList<OperationalIncident> GetRecent(int count = 50)
    {
        lock (_lock)
            return _incidents.Take(count).ToList();
    }

    public IReadOnlyList<OperationalIncident> GetUnresolved()
    {
        lock (_lock)
            return _incidents.Where(i => !i.IsResolved).ToList();
    }
}
