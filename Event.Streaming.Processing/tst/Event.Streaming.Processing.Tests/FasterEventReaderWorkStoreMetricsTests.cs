using Event.Streaming.Processing.WorkStore;

namespace Event.Streaming.Processing.Tests;

public sealed class FasterEventReaderWorkStoreMetricsTests : IDisposable
{
    private readonly string _testDir;
    private readonly FasterEventReaderWorkStore _store;
    private readonly FasterEventReaderWorkStoreOptions _options;

    public FasterEventReaderWorkStoreMetricsTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "faster-metrics-tests", Guid.NewGuid().ToString("N"));
        _options = new FasterEventReaderWorkStoreOptions
        {
            LogPath = Path.Combine(_testDir, "work-log"),
            CheckpointPath = Path.Combine(_testDir, "checkpoints"),
            CheckpointIntervalMs = 0,
        };
        _store = new FasterEventReaderWorkStore(_options);
    }

    public void Dispose()
    {
        _store.Dispose();
        try { Directory.Delete(_testDir, recursive: true); } catch { }
    }

    [Fact]
    public async Task GetDetailedMetrics_backlog_by_state_shows_correct_counts()
    {
        // Enqueue 3 items, all in Classified state
        await _store.EnqueueClassifiedAsync(CreateItem(shardId: 1), CancellationToken.None);
        await _store.EnqueueClassifiedAsync(CreateItem(shardId: 1, offset: 2), CancellationToken.None);
        await _store.EnqueueClassifiedAsync(CreateItem(shardId: 1, offset: 3), CancellationToken.None);

        // Lease one item -> moves to Processing (use maxItems: 1)
        var leases = await _store.LeaseShardBatchAsync(1, 1, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Single(leases);

        // Lease another -> also Processing
        var leases2 = await _store.LeaseShardBatchAsync(1, 1, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Single(leases2);

        // Mark one ready -> ReadyToOutput
        await _store.MarkReadyToOutputAsync(leases[0].WorkItemId, [1, 2], CancellationToken.None);

        var metrics = _store.GetDetailedMetrics();

        Assert.Equal(1, metrics.BacklogByState[WorkState.Classified]);
        Assert.Equal(1, metrics.BacklogByState[WorkState.Processing]);
        Assert.Equal(1, metrics.BacklogByState[WorkState.ReadyToOutput]);
        Assert.Equal(0, metrics.BacklogByState[WorkState.Publishing]);
        Assert.Equal(0, metrics.BacklogByState[WorkState.Completed]);
        Assert.Equal(0, metrics.BacklogByState[WorkState.Failed]);
        Assert.Equal(0, metrics.BacklogByState[WorkState.RetryPending]);
    }

    [Fact]
    public async Task GetDetailedMetrics_backlog_by_shard_counts_unfinished_items()
    {
        await _store.EnqueueClassifiedAsync(CreateItem(shardId: 10), CancellationToken.None);
        await _store.EnqueueClassifiedAsync(CreateItem(shardId: 10, offset: 2), CancellationToken.None);
        await _store.EnqueueClassifiedAsync(CreateItem(shardId: 20, offset: 3), CancellationToken.None);
        await _store.EnqueueClassifiedAsync(CreateItem(shardId: 30, offset: 4), CancellationToken.None);

        // Complete one of the shard-10 items so it doesn't count as unfinished
        var leases = await _store.LeaseShardBatchAsync(10, 1, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Single(leases);
        await _store.MarkReadyToOutputAsync(leases[0].WorkItemId, [1], CancellationToken.None);
        var outLeases = await _store.LeaseOutputBatchAsync(10, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Single(outLeases);
        await _store.MarkCompletedAsync(
            outLeases[0].WorkItemId,
            new OutputWriteReceipt("t", 0, 100, DateTimeOffset.UtcNow, null),
            CancellationToken.None);

        var metrics = _store.GetDetailedMetrics();

        Assert.Equal(1, metrics.BacklogByShard[10]); // one unfinished left in shard 10
        Assert.Equal(1, metrics.BacklogByShard[20]);
        Assert.Equal(1, metrics.BacklogByShard[30]);
    }

    [Fact]
    public async Task GetDetailedMetrics_oldest_unfinished_age_is_reported()
    {
        await _store.EnqueueClassifiedAsync(CreateItem(shardId: 1), CancellationToken.None);

        var metrics = _store.GetDetailedMetrics();

        Assert.NotNull(metrics.OldestUnfinishedAge);
        Assert.True(metrics.OldestUnfinishedAge!.Value.TotalMilliseconds >= 0);
    }

    [Fact]
    public async Task GetDetailedMetrics_latency_values_are_populated()
    {
        await _store.EnqueueClassifiedAsync(CreateItem(shardId: 1), CancellationToken.None);
        var leases = await _store.LeaseShardBatchAsync(1, 10, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Single(leases);
        await _store.MarkReadyToOutputAsync(leases[0].WorkItemId, [1], CancellationToken.None);
        var outLeases = await _store.LeaseOutputBatchAsync(10, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Single(outLeases);

        var metrics = _store.GetDetailedMetrics();

        Assert.True(metrics.AverageEnqueueLatencyMs >= 0);
        Assert.True(metrics.AverageShardLeaseLatencyMs >= 0);
        Assert.True(metrics.AverageOutputLeaseLatencyMs >= 0);
    }

    [Fact]
    public async Task GetDetailedMetrics_completed_and_failed_terminate_backlog()
    {
        await _store.EnqueueClassifiedAsync(CreateItem(shardId: 1), CancellationToken.None);
        await _store.EnqueueClassifiedAsync(CreateItem(shardId: 1, offset: 2), CancellationToken.None);

        // Mark first as failed
        await _store.MarkFailedAsync(1, "Permanent error", CancellationToken.None);

        // Complete second through full pipeline
        var leases = await _store.LeaseShardBatchAsync(1, 10, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Single(leases);
        await _store.MarkReadyToOutputAsync(leases[0].WorkItemId, [1], CancellationToken.None);
        var outLeases = await _store.LeaseOutputBatchAsync(10, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Single(outLeases);
        await _store.MarkCompletedAsync(
            outLeases[0].WorkItemId,
            new OutputWriteReceipt("t", 0, 100, DateTimeOffset.UtcNow, null),
            CancellationToken.None);

        var metrics = _store.GetDetailedMetrics();

        Assert.Equal(1, metrics.BacklogByState[WorkState.Completed]);
        Assert.Equal(1, metrics.BacklogByState[WorkState.Failed]);
        Assert.Empty(metrics.BacklogByShard); // no unfinished items
        Assert.Null(metrics.OldestUnfinishedAge);       // nothing unfinished
    }

    private static ClassifiedWorkItem CreateItem(int shardId, long offset = 42) =>
        new(
            0,
            new KafkaSourceIdentity("test", "topic", 0, offset, DateTimeOffset.UtcNow),
            100,
            12345,
            shardId,
            1,
            1,
            1,
            1,
            1,
            [1, 2, 3],
            DateTimeOffset.UtcNow);
}
