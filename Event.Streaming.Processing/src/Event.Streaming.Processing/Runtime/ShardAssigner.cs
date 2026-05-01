namespace Event.Streaming.Processing.Runtime;

public sealed class ShardAssigner
{
    private const ulong FnvOffsetBasis = 14695981039346656037UL;
    private const ulong FnvPrime = 1099511628211UL;

    public ShardAssigner(int logicalShardCount)
    {
        if (logicalShardCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(logicalShardCount), "Logical shard count must be positive.");
        }

        LogicalShardCount = logicalShardCount;
    }

    public int LogicalShardCount { get; }

    public ShardAssignment Assign(long nedbankId)
    {
        var hash = StableHash(nedbankId);
        var shardId = (int)(hash % (ulong)LogicalShardCount);
        return new ShardAssignment(nedbankId, hash, shardId);
    }

    public static int GetWorkerIndex(int shardId, int activeWorkerCount)
    {
        if (shardId < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(shardId), "Shard ID must be non-negative.");
        }

        if (activeWorkerCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(activeWorkerCount), "Active worker count must be positive.");
        }

        return shardId % activeWorkerCount;
    }

    public static ulong StableHash(long value)
    {
        unchecked
        {
            var hash = FnvOffsetBasis;
            var unsigned = (ulong)value;
            for (var i = 0; i < sizeof(long); i++)
            {
                hash ^= (byte)(unsigned >> (i * 8));
                hash *= FnvPrime;
            }

            return hash;
        }
    }
}

public sealed record ShardAssignment(long NedbankId, ulong StableHash, int ShardId);
