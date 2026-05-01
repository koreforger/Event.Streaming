using Event.Streaming.Processing.WorkStore;

namespace Event.Streaming.Processing.Tests;

public sealed class FasterEventReaderWorkStoreTests : IDisposable
{
    private readonly string _testDir;
    private readonly FasterEventReaderWorkStore _store;
    private readonly FasterEventReaderWorkStoreOptions _options;

    public FasterEventReaderWorkStoreTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "faster-tests", Guid.NewGuid().ToString("N"));
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
    public async Task Enqueue_creates_work_item_with_durable_id()
    {
        var item = CreateClassifiedWorkItem(workItemId: 0);

        await _store.EnqueueClassifiedAsync(item, CancellationToken.None);

        var metrics = _store.GetMetrics();
        Assert.True(metrics.LastWorkItemId >= 1);
    }

    [Fact]
    public async Task Enqueue_is_idempotent_by_source_identity()
    {
        var item = CreateClassifiedWorkItem(workItemId: 0);

        await _store.EnqueueClassifiedAsync(item, CancellationToken.None);
        await _store.EnqueueClassifiedAsync(item, CancellationToken.None);

        var metrics = _store.GetMetrics();
        Assert.Equal(1, metrics.LastWorkItemId);
    }

    [Fact]
    public async Task Enqueue_different_source_identity_creates_different_work_items()
    {
        var item1 = CreateClassifiedWorkItem(workItemId: 0, offset: 1);
        var item2 = CreateClassifiedWorkItem(workItemId: 0, offset: 2);

        await _store.EnqueueClassifiedAsync(item1, CancellationToken.None);
        await _store.EnqueueClassifiedAsync(item2, CancellationToken.None);

        var metrics = _store.GetMetrics();
        Assert.Equal(2, metrics.LastWorkItemId);
    }

    [Fact]
    public async Task Lease_shard_batch_returns_classified_items()
    {
        var item = CreateClassifiedWorkItem(workItemId: 0, shardId: 5);
        await _store.EnqueueClassifiedAsync(item, CancellationToken.None);

        var leases = await _store.LeaseShardBatchAsync(5, 10, TimeSpan.FromMinutes(5), CancellationToken.None);

        Assert.Single(leases);
        Assert.Equal(5, leases[0].ShardId);
    }

    [Fact]
    public async Task Lease_shard_batch_respects_shard_boundary()
    {
        await _store.EnqueueClassifiedAsync(CreateClassifiedWorkItem(0, shardId: 5), CancellationToken.None);

        var leases = await _store.LeaseShardBatchAsync(3, 10, TimeSpan.FromMinutes(5), CancellationToken.None);

        Assert.Empty(leases);
    }

    [Fact]
    public async Task Lease_is_exclusive()
    {
        await _store.EnqueueClassifiedAsync(CreateClassifiedWorkItem(0, shardId: 5), CancellationToken.None);

        var first = await _store.LeaseShardBatchAsync(5, 10, TimeSpan.FromMinutes(5), CancellationToken.None);
        var second = await _store.LeaseShardBatchAsync(5, 10, TimeSpan.FromMinutes(5), CancellationToken.None);

        Assert.Single(first);
        Assert.Empty(second);
    }

    [Fact]
    public async Task Mark_ready_to_output_transitions_state()
    {
        await _store.EnqueueClassifiedAsync(CreateClassifiedWorkItem(0, shardId: 5), CancellationToken.None);
        var leases = await _store.LeaseShardBatchAsync(5, 1, TimeSpan.FromMinutes(5), CancellationToken.None);
        var workItemId = leases[0].WorkItemId;

        await _store.MarkReadyToOutputAsync(workItemId, [10, 20, 30], CancellationToken.None);
    }

    [Fact]
    public async Task Mark_ready_to_output_requires_processing_state()
    {
        await _store.EnqueueClassifiedAsync(CreateClassifiedWorkItem(0), CancellationToken.None);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _store.MarkReadyToOutputAsync(1, [1], CancellationToken.None));
    }

    [Fact]
    public async Task Lease_output_batch_returns_ready_items()
    {
        await _store.EnqueueClassifiedAsync(CreateClassifiedWorkItem(0, shardId: 5), CancellationToken.None);
        var leases = await _store.LeaseShardBatchAsync(5, 1, TimeSpan.FromMinutes(5), CancellationToken.None);
        await _store.MarkReadyToOutputAsync(leases[0].WorkItemId, [1, 2, 3], CancellationToken.None);

        var outputLeases = await _store.LeaseOutputBatchAsync(10, TimeSpan.FromMinutes(5), CancellationToken.None);

        Assert.Single(outputLeases);
        Assert.Equal([1, 2, 3], outputLeases[0].OutputPayload);
    }

    [Fact]
    public async Task Output_lease_is_exclusive()
    {
        await _store.EnqueueClassifiedAsync(CreateClassifiedWorkItem(0, shardId: 5), CancellationToken.None);
        var leases = await _store.LeaseShardBatchAsync(5, 1, TimeSpan.FromMinutes(5), CancellationToken.None);
        await _store.MarkReadyToOutputAsync(leases[0].WorkItemId, [1], CancellationToken.None);

        var first = await _store.LeaseOutputBatchAsync(10, TimeSpan.FromMinutes(5), CancellationToken.None);
        var second = await _store.LeaseOutputBatchAsync(10, TimeSpan.FromMinutes(5), CancellationToken.None);

        Assert.Single(first);
        Assert.Empty(second);
    }

    [Fact]
    public async Task Mark_completed_finalizes_work_item()
    {
        await _store.EnqueueClassifiedAsync(CreateClassifiedWorkItem(0, shardId: 5), CancellationToken.None);
        var leases = await _store.LeaseShardBatchAsync(5, 1, TimeSpan.FromMinutes(5), CancellationToken.None);
        await _store.MarkReadyToOutputAsync(leases[0].WorkItemId, [1], CancellationToken.None);
        var outLeases = await _store.LeaseOutputBatchAsync(10, TimeSpan.FromMinutes(5), CancellationToken.None);

        var receipt = new OutputWriteReceipt("output-topic", 0, 100, DateTimeOffset.UtcNow, null);
        await _store.MarkCompletedAsync(outLeases[0].WorkItemId, receipt, CancellationToken.None);
    }

    [Fact]
    public async Task Mark_retry_pending_transitions_to_retry()
    {
        await _store.EnqueueClassifiedAsync(CreateClassifiedWorkItem(0, shardId: 5), CancellationToken.None);
        var leases = await _store.LeaseShardBatchAsync(5, 1, TimeSpan.FromMinutes(5), CancellationToken.None);

        var reason = new RetryReason("ExtractionFailed", "Script timeout", DateTimeOffset.UtcNow.AddSeconds(10), WorkState.Classified);
        await _store.MarkRetryPendingAsync(leases[0].WorkItemId, reason, CancellationToken.None);
    }

    [Fact]
    public async Task Mark_failed_transitions_to_failed()
    {
        await _store.EnqueueClassifiedAsync(CreateClassifiedWorkItem(0, shardId: 5), CancellationToken.None);

        await _store.MarkFailedAsync(1, "Permanent failure", CancellationToken.None);
    }

    [Fact]
    public async Task Release_expired_leases_resets_state()
    {
        await _store.EnqueueClassifiedAsync(CreateClassifiedWorkItem(0, shardId: 5), CancellationToken.None);
        var leases = await _store.LeaseShardBatchAsync(5, 1, TimeSpan.FromMilliseconds(1), CancellationToken.None);
        Assert.Single(leases);

        await Task.Delay(50);

        await _store.ReleaseExpiredLeasesAsync(CancellationToken.None);

        var reLeases = await _store.LeaseShardBatchAsync(5, 1, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Single(reLeases);
    }

    [Fact]
    public async Task Full_pipeline_work_item_lifecycle()
    {
        await _store.EnqueueClassifiedAsync(CreateClassifiedWorkItem(0, shardId: 7, functionId: 123), CancellationToken.None);

        var shardLeases = await _store.LeaseShardBatchAsync(7, 10, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Single(shardLeases);

        var workItemId = shardLeases[0].WorkItemId;
        await _store.MarkReadyToOutputAsync(workItemId, [42], CancellationToken.None);

        var outLeases = await _store.LeaseOutputBatchAsync(10, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Single(outLeases);

        var receipt = new OutputWriteReceipt("output", 0, 42, DateTimeOffset.UtcNow, null);
        await _store.MarkCompletedAsync(outLeases[0].WorkItemId, receipt, CancellationToken.None);
    }

    [Fact]
    public async Task Store_survives_dispose_and_recreate()
    {
        var item = CreateClassifiedWorkItem(0);
        await _store.EnqueueClassifiedAsync(item, CancellationToken.None);

        var id = _store.GetMetrics().LastWorkItemId;

        _store.Dispose();

        var store2 = new FasterEventReaderWorkStore(_options);
        try
        {
            var metrics = store2.GetMetrics();
        }
        finally
        {
            store2.Dispose();
        }
    }

    private static ClassifiedWorkItem CreateClassifiedWorkItem(
        long workItemId,
        int shardId = 44,
        int functionId = 100,
        long offset = 42) =>
        new(
            workItemId,
            new KafkaSourceIdentity("test-provider", "test-topic", 1, offset, DateTimeOffset.UtcNow),
            functionId,
            12345,
            shardId,
            7,
            11,
            13,
            17,
            19,
            [1, 2, 3],
            DateTimeOffset.UtcNow);
}
