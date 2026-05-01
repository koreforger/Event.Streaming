using System.Reflection;
using Event.Streaming.Processing.Runtime;
using Event.Streaming.Processing.WorkStore;

namespace Event.Streaming.Processing.Tests;

public sealed class EventReaderWorkStoreContractTests
{
    [Fact]
    public void Kafka_source_identity_equality_includes_source_system_topic_partition_and_offset()
    {
        var first = new KafkaSourceIdentity("provider-a", "ProviderA.Payments", 1, 42, DateTimeOffset.Parse("2026-04-28T10:00:00Z"));
        var same = new KafkaSourceIdentity("provider-a", "ProviderA.Payments", 1, 42, DateTimeOffset.Parse("2026-04-28T10:00:00Z"));
        var differentOffset = first with { Offset = 43 };

        Assert.Equal(first, same);
        Assert.NotEqual(first, differentOffset);
    }

    [Fact]
    public void Classified_work_item_captures_source_identity_versions_and_shard()
    {
        var source = new KafkaSourceIdentity("provider-a", "ProviderA.Payments", 1, 42, null);
        var item = new ClassifiedWorkItem(
            1001,
            source,
            123,
            456789,
            44,
            7,
            11,
            13,
            17,
            19,
            [1, 2, 3],
            DateTimeOffset.Parse("2026-04-28T10:00:00Z"));

        Assert.Equal(1001, item.WorkItemId);
        Assert.Equal(source, item.Source);
        Assert.Equal(123, item.FunctionId);
        Assert.Equal(456789, item.NedbankId);
        Assert.Equal(44, item.ShardId);
        Assert.Equal(7, item.RuntimeModelVersion);
        Assert.Equal(11, item.FunctionVersion);
        Assert.Equal(13, item.ExtractionScriptVersion);
        Assert.Equal(17, item.RuleSetVersion);
        Assert.Equal(19, item.OutputRouteVersion);
        Assert.Equal([1, 2, 3], item.RawPayload);
    }

    [Fact]
    public void Work_state_contains_required_durable_states()
    {
        var states = Enum.GetNames<WorkState>();

        Assert.Contains(nameof(WorkState.Classified), states);
        Assert.Contains(nameof(WorkState.Processing), states);
        Assert.Contains(nameof(WorkState.ReadyToOutput), states);
        Assert.Contains(nameof(WorkState.Publishing), states);
        Assert.Contains(nameof(WorkState.Completed), states);
        Assert.Contains(nameof(WorkState.RetryPending), states);
        Assert.Contains(nameof(WorkState.Failed), states);
        Assert.Contains(nameof(WorkState.Suppressed), states);
    }

    [Fact]
    public void Work_store_contract_exposes_required_operations()
    {
        var methods = typeof(IEventReaderWorkStore)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Select(method => method.Name)
            .ToArray();

        Assert.Contains(nameof(IEventReaderWorkStore.EnqueueClassifiedAsync), methods);
        Assert.Contains(nameof(IEventReaderWorkStore.LeaseShardBatchAsync), methods);
        Assert.Contains(nameof(IEventReaderWorkStore.MarkReadyToOutputAsync), methods);
        Assert.Contains(nameof(IEventReaderWorkStore.LeaseOutputBatchAsync), methods);
        Assert.Contains(nameof(IEventReaderWorkStore.MarkCompletedAsync), methods);
        Assert.Contains(nameof(IEventReaderWorkStore.MarkRetryPendingAsync), methods);
        Assert.Contains(nameof(IEventReaderWorkStore.MarkFailedAsync), methods);
        Assert.Contains(nameof(IEventReaderWorkStore.ReleaseExpiredLeasesAsync), methods);
        Assert.Contains(nameof(IEventReaderWorkStore.CreateReplayPlanAsync), methods);
        Assert.Contains(nameof(IEventReaderWorkStore.RequeueAsync), methods);
    }
}
