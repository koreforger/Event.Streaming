using Event.Streaming.Processing.Runtime;

namespace Event.Streaming.Processing.Tests;

public sealed class ShardAssignerTests
{
    [Fact]
    public void Assign_returns_same_shard_for_same_nedbank_id()
    {
        var assigner = new ShardAssigner(logicalShardCount: 1024);

        var first = assigner.Assign(123456789);
        var second = assigner.Assign(123456789);

        Assert.Equal(first.ShardId, second.ShardId);
        Assert.InRange(first.ShardId, 0, 1023);
    }

    [Fact]
    public void Assign_distributes_nedbank_ids_across_shards()
    {
        var assigner = new ShardAssigner(logicalShardCount: 128);

        var populatedShardCount = Enumerable.Range(1, 10_000)
            .Select(i => assigner.Assign(i).ShardId)
            .Distinct()
            .Count();

        Assert.True(populatedShardCount >= 120);
    }

    [Fact]
    public void Worker_mapping_can_change_without_changing_stored_shard_id()
    {
        var assigner = new ShardAssigner(logicalShardCount: 1024);
        var assignment = assigner.Assign(987654321);

        var workerWith16 = ShardAssigner.GetWorkerIndex(assignment.ShardId, activeWorkerCount: 16);
        var workerWith32 = ShardAssigner.GetWorkerIndex(assignment.ShardId, activeWorkerCount: 32);

        Assert.Equal(assignment.ShardId, assigner.Assign(987654321).ShardId);
        Assert.Equal(assignment.ShardId % 16, workerWith16);
        Assert.Equal(assignment.ShardId % 32, workerWith32);
    }
}
