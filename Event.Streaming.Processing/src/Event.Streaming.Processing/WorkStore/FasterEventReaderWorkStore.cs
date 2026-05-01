using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using FASTER.core;

namespace Event.Streaming.Processing.WorkStore;

public sealed class FasterEventReaderWorkStore : IEventReaderWorkStore, IDisposable
{
    private const int WorkItemIdReserveBatch = 4096;
    private const int CheckpointAfterOperations = 100_000;
    private const string DurableSequenceWorkItemId = "D:WorkItemId";
    private const string DurableSequenceQueue = "D:QueueSequence";

    private readonly FasterKV<string, byte[]> _store;
    private readonly IDevice _log;
    private readonly IDevice _objLog;
    private readonly ClientSession<string, byte[], byte[], byte[], Empty, SimpleFunctions<string, byte[], Empty>> _session;
    private readonly FasterEventReaderWorkStoreOptions _options;
    private readonly string _ownerId;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly Timer? _checkpointTimer;

    private long _nextWorkItemId;
    private long _workItemIdLimit;
    private long _nextQueueSequence;
    private long _nextQueueLimit;
    private long _operationCount;
    private long _enqueueLatencyTicks;
    private long _enqueueCount;
    private long _shardLeaseLatencyTicks;
    private long _shardLeaseCount;
    private long _outputLeaseLatencyTicks;
    private long _outputLeaseCount;
    private DateTimeOffset? _lastCheckpointTime;
    private bool _disposed;

    public FasterEventReaderWorkStore(FasterEventReaderWorkStoreOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _ownerId = $"{Environment.MachineName}:{Environment.ProcessId}:{Guid.NewGuid():N}";

        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };

        Directory.CreateDirectory(options.LogPath);
        Directory.CreateDirectory(options.CheckpointPath);

        _log = Devices.CreateLogDevice(Path.Combine(options.LogPath, "work-store.log"));
        _objLog = Devices.CreateLogDevice(Path.Combine(options.LogPath, "work-store.obj.log"));

        _store = new FasterKV<string, byte[]>(
            options.IndexSizeBuckets,
            logSettings: new LogSettings
            {
                LogDevice = _log,
                ObjectLogDevice = _objLog,
                PageSizeBits = options.LogPageSizeBits,
                MemorySizeBits = options.LogMemorySizeBits,
            },
            checkpointSettings: new CheckpointSettings
            {
                CheckpointDir = options.CheckpointPath,
            });

        var functions = new SimpleFunctions<string, byte[], Empty>();
        _session = _store.For(functions).NewSession<SimpleFunctions<string, byte[], Empty>>();

        RecoverOrBootstrap();

        if (options.CheckpointIntervalMs > 0)
        {
            _checkpointTimer = new Timer(
                _ => _ = CheckpointLoopAsync(),
                null,
                options.CheckpointIntervalMs,
                options.CheckpointIntervalMs);
        }
    }

    public async Task EnqueueClassifiedAsync(ClassifiedWorkItem item, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(item);

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var sw = Stopwatch.StartNew();
            var sourceKey = MakeSourceIndexKey(item.Source);
            byte[]? sourceOut = new byte[0];
            var sourceStatus = _session.Read(ref sourceKey, ref sourceOut);

            if (sourceStatus.Found)
            {
                return;
            }

            var workItemId = AllocateWorkItemId();
            var now = DateTimeOffset.UtcNow;
            var sequence = AllocateQueueSequence();

            var record = new WorkItemRecord
            {
                WorkItemId = workItemId,
                SourceSystemId = item.Source.SourceSystemId,
                Topic = item.Source.Topic,
                Partition = item.Source.Partition,
                Offset = item.Source.Offset,
                KafkaTimestampUtc = item.Source.KafkaTimestampUtc?.ToString("O"),
                FunctionId = item.FunctionId,
                NedbankId = item.NedbankId,
                ShardId = item.ShardId,
                RuntimeModelVersion = item.RuntimeModelVersion,
                FunctionVersion = item.FunctionVersion,
                ExtractionScriptVersion = item.ExtractionScriptVersion,
                RuleSetVersion = item.RuleSetVersion,
                OutputRouteVersion = item.OutputRouteVersion,
                State = (int)WorkState.Classified,
                PreviousState = -1,
                ProcessingAttempt = 0,
                OutputAttempt = 0,
                RawPayload = Convert.ToBase64String(item.RawPayload),
                CreatedUtc = now.ToString("O"),
                UpdatedUtc = now.ToString("O"),
            };

            var workItemKey = MakeWorkItemKey(workItemId);
            var stateQueueKey = MakeStateQueueKey(WorkState.Classified, sequence);
            var shardQueueKey = MakeShardQueueKey(item.ShardId, sequence);

            await WriteJsonAsync(workItemKey, record).ConfigureAwait(false);
            await WriteSourceIndexAsync(sourceKey, workItemId).ConfigureAwait(false);
            await WriteJsonAsync(stateQueueKey, new QueueValue { WorkItemId = workItemId }).ConfigureAwait(false);
            await WriteJsonAsync(shardQueueKey, new QueueValue { WorkItemId = workItemId }).ConfigureAwait(false);

            _operationCount += 4;

            PersistDurableSequenceIfNeeded();
            CheckpointIfNeeded();
            sw.Stop();
            _enqueueLatencyTicks += sw.ElapsedTicks;
            _enqueueCount++;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<WorkLease>> LeaseShardBatchAsync(
        int shardId,
        int maxItems,
        TimeSpan leaseDuration,
        CancellationToken ct)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var sw = Stopwatch.StartNew();
            var leases = new List<WorkLease>();
            var prefix = MakeShardQueuePrefix(shardId);
            var now = DateTimeOffset.UtcNow;
            var expiry = now + leaseDuration;

            using var iter = _session.Iterate();
            while (iter.GetNext(out _) && leases.Count < maxItems)
            {
                var key = iter.GetKey();
                if (!key.StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                var queueValue = ReadQueueValue(iter.GetValue(), _jsonOptions);
                if (queueValue is null)
                {
                    continue;
                }

                var workItemKey = MakeWorkItemKey(queueValue.WorkItemId);
                var record = await ReadJsonAsync<WorkItemRecord>(workItemKey).ConfigureAwait(false);
                if (record is null || record.State != (int)WorkState.Classified)
                {
                    continue;
                }

                record.State = (int)WorkState.Processing;
                record.PreviousState = (int)WorkState.Classified;
                record.ProcessingAttempt++;
                record.LeaseOwnerId = _ownerId;
                record.LeaseExpiresUtc = expiry.ToString("O");
                record.UpdatedUtc = now.ToString("O");
                await WriteJsonAsync(workItemKey, record).ConfigureAwait(false);

                leases.Add(new WorkLease(
                    queueValue.WorkItemId,
                    shardId,
                    _ownerId,
                    expiry,
                    record.ProcessingAttempt,
                    new ClassifiedWorkItem(
                        record.WorkItemId,
                        new KafkaSourceIdentity(
                            record.SourceSystemId,
                            record.Topic,
                            record.Partition,
                            record.Offset,
                            ParseDateTimeOffset(record.KafkaTimestampUtc)),
                        record.FunctionId,
                        record.NedbankId,
                        record.ShardId,
                        record.RuntimeModelVersion,
                        record.FunctionVersion,
                        record.ExtractionScriptVersion,
                        record.RuleSetVersion,
                        record.OutputRouteVersion,
                        !string.IsNullOrEmpty(record.RawPayload)
                            ? Convert.FromBase64String(record.RawPayload)
                            : [],
                        ParseDateTimeOffset(record.CreatedUtc) ?? DateTimeOffset.MinValue)));

                _operationCount++;
            }

            CheckpointIfNeeded();
            sw.Stop();
            _shardLeaseLatencyTicks += sw.ElapsedTicks;
            _shardLeaseCount++;
            return leases;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task MarkReadyToOutputAsync(
        long workItemId,
        byte[] outputPayload,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(outputPayload);

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var key = MakeWorkItemKey(workItemId);
            var record = await ReadJsonAsync<WorkItemRecord>(key).ConfigureAwait(false)
                ?? throw new InvalidOperationException($"WorkItem {workItemId} not found.");

            if (record.State != (int)WorkState.Processing)
            {
                throw new InvalidOperationException(
                    $"WorkItem {workItemId} is in state {(WorkState)record.State}, expected Processing.");
            }

            var sequence = AllocateQueueSequence();
            var now = DateTimeOffset.UtcNow;

            record.State = (int)WorkState.ReadyToOutput;
            record.PreviousState = (int)WorkState.Processing;
            record.OutputPayload = Convert.ToBase64String(outputPayload);
            record.UpdatedUtc = now.ToString("O");
            await WriteJsonAsync(key, record).ConfigureAwait(false);

            var outputQueueKey = MakeOutputQueueKey(sequence);
            await WriteJsonAsync(outputQueueKey, new QueueValue { WorkItemId = workItemId }).ConfigureAwait(false);

            _operationCount += 2;
            CheckpointIfNeeded();
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<OutputLease>> LeaseOutputBatchAsync(
        int maxItems,
        TimeSpan leaseDuration,
        CancellationToken ct)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var sw = Stopwatch.StartNew();
            var leases = new List<OutputLease>();
            var prefix = MakeOutputQueuePrefix();
            var now = DateTimeOffset.UtcNow;
            var expiry = now + leaseDuration;

            using var iter = _session.Iterate();
            while (iter.GetNext(out _) && leases.Count < maxItems)
            {
                var key = iter.GetKey();
                if (!key.StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                var queueValue = ReadQueueValue(iter.GetValue(), _jsonOptions);
                if (queueValue is null)
                {
                    continue;
                }

                var workItemKey = MakeWorkItemKey(queueValue.WorkItemId);
                var record = await ReadJsonAsync<WorkItemRecord>(workItemKey).ConfigureAwait(false);
                if (record is null || record.State != (int)WorkState.ReadyToOutput)
                {
                    continue;
                }

                record.State = (int)WorkState.Publishing;
                record.PreviousState = (int)WorkState.ReadyToOutput;
                record.OutputAttempt++;
                record.LeaseOwnerId = _ownerId;
                record.LeaseExpiresUtc = expiry.ToString("O");
                record.UpdatedUtc = now.ToString("O");
                await WriteJsonAsync(workItemKey, record).ConfigureAwait(false);

                leases.Add(new OutputLease(
                    queueValue.WorkItemId,
                    _ownerId,
                    expiry,
                    record.OutputAttempt,
                    !string.IsNullOrEmpty(record.OutputPayload)
                        ? Convert.FromBase64String(record.OutputPayload)
                        : []));

                _operationCount++;
            }

            CheckpointIfNeeded();
            sw.Stop();
            _outputLeaseLatencyTicks += sw.ElapsedTicks;
            _outputLeaseCount++;
            return leases;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task MarkCompletedAsync(
        long workItemId,
        OutputWriteReceipt receipt,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(receipt);

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var key = MakeWorkItemKey(workItemId);
            var record = await ReadJsonAsync<WorkItemRecord>(key).ConfigureAwait(false)
                ?? throw new InvalidOperationException($"WorkItem {workItemId} not found.");

            if (record.State != (int)WorkState.Publishing)
            {
                throw new InvalidOperationException(
                    $"WorkItem {workItemId} is in state {(WorkState)record.State}, expected Publishing.");
            }

            var now = DateTimeOffset.UtcNow;

            record.State = (int)WorkState.Completed;
            record.PreviousState = (int)WorkState.Publishing;
            record.CompletedUtc = now.ToString("O");
            record.UpdatedUtc = now.ToString("O");
            await WriteJsonAsync(key, record).ConfigureAwait(false);

            _operationCount++;
            CheckpointIfNeeded();
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task MarkRetryPendingAsync(
        long workItemId,
        RetryReason reason,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(reason);

        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var key = MakeWorkItemKey(workItemId);
            var record = await ReadJsonAsync<WorkItemRecord>(key).ConfigureAwait(false)
                ?? throw new InvalidOperationException($"WorkItem {workItemId} not found.");

            var now = DateTimeOffset.UtcNow;

            var previousState = record.State;
            record.State = (int)WorkState.RetryPending;
            record.PreviousState = previousState;
            record.LastError = $"{reason.Category}: {reason.Message}";
            record.UpdatedUtc = now.ToString("O");
            await WriteJsonAsync(key, record).ConfigureAwait(false);

            _operationCount++;
            CheckpointIfNeeded();
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task MarkFailedAsync(
        long workItemId,
        string reason,
        CancellationToken ct)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var key = MakeWorkItemKey(workItemId);
            var record = await ReadJsonAsync<WorkItemRecord>(key).ConfigureAwait(false)
                ?? throw new InvalidOperationException($"WorkItem {workItemId} not found.");

            var now = DateTimeOffset.UtcNow;

            var previousState = record.State;
            record.State = (int)WorkState.Failed;
            record.PreviousState = previousState;
            record.LastError = reason;
            record.UpdatedUtc = now.ToString("O");
            await WriteJsonAsync(key, record).ConfigureAwait(false);

            _operationCount++;
            CheckpointIfNeeded();
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task ReleaseExpiredLeasesAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var now = DateTimeOffset.UtcNow;
            var prefix = MakeWorkItemPrefix();

            using var iter = _session.Iterate();
            while (iter.GetNext(out _))
            {
                var key = iter.GetKey();
                if (!key.StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                var record = await ReadJsonAsync<WorkItemRecord>(key).ConfigureAwait(false);
                if (record is null)
                {
                    continue;
                }

                if (record.State is not ((int)WorkState.Processing) and not ((int)WorkState.Publishing))
                {
                    continue;
                }

                var leaseExpiry = ParseDateTimeOffset(record.LeaseExpiresUtc);
                if (leaseExpiry >= now)
                {
                    continue;
                }

                var previousState = record.State switch
                {
                    (int)WorkState.Processing => (int)WorkState.Classified,
                    (int)WorkState.Publishing => (int)WorkState.ReadyToOutput,
                    _ => record.State,
                };

                record.PreviousState = record.State;
                record.State = previousState;
                record.LeaseOwnerId = null;
                record.LeaseExpiresUtc = null;
                record.UpdatedUtc = now.ToString("O");
                await WriteJsonAsync(key, record).ConfigureAwait(false);
            }

            CheckpointIfNeeded();
        }
        finally
        {
            _gate.Release();
        }
    }

    public Task<ReplayPlan> CreateReplayPlanAsync(
        ReplayRequest request,
        CancellationToken ct)
    {
        return Task.FromResult(new ReplayPlan(
            Guid.NewGuid(),
            request,
            [],
            DateTimeOffset.UtcNow));
    }

    public Task<UnstuckResult> RequeueAsync(
        UnstuckRequest request,
        CancellationToken ct)
    {
        return Task.FromResult(new UnstuckResult(
            request.WorkItemIds.Count,
            0,
            []));
    }

    public FasterEventReaderWorkStoreMetrics GetMetrics()
    {
        return new FasterEventReaderWorkStoreMetrics(
            _nextWorkItemId - 1,
            _operationCount,
            _options.LogPath,
            _options.CheckpointPath);
    }

    public FasterEventReaderWorkStoreDetailedMetrics GetDetailedMetrics()
    {
        var stateCounts = new Dictionary<WorkState, long>();
        foreach (var ws in Enum.GetValues<WorkState>())
        {
            stateCounts[ws] = 0;
        }

        var shardCounts = new Dictionary<int, long>();
        var activeModelVersions = new HashSet<long>();
        DateTimeOffset? oldestCreated = null;
        var prefix = MakeWorkItemPrefix();

        using var iter = _session.Iterate();
        while (iter.GetNext(out _))
        {
            var key = iter.GetKey();
            if (!key.StartsWith(prefix, StringComparison.Ordinal))
            {
                continue;
            }

            try
            {
                var record = ReadWorkItemRecord(iter.GetValue(), _jsonOptions);
                if (record is null)
                {
                    continue;
                }

                var state = (WorkState)record.State;
                stateCounts[state] = stateCounts.TryGetValue(state, out var c) ? c + 1 : 1;

                var isFinished = state is WorkState.Completed or WorkState.Failed or WorkState.Suppressed;
                if (!isFinished)
                {
                    if (!string.IsNullOrEmpty(record.CreatedUtc))
                    {
                        var created = ParseDateTimeOffset(record.CreatedUtc);
                        if (created.HasValue && (!oldestCreated.HasValue || created.Value < oldestCreated.Value))
                        {
                            oldestCreated = created.Value;
                        }
                    }

                    shardCounts[record.ShardId] = shardCounts.TryGetValue(record.ShardId, out var sc) ? sc + 1 : 1;
                    activeModelVersions.Add(record.RuntimeModelVersion);
                }
            }
            catch
            {
            }
        }

        var oldestAge = oldestCreated.HasValue
            ? DateTimeOffset.UtcNow - oldestCreated.Value
            : (TimeSpan?)null;

        long diskFree = 0;
        long diskTotal = 0;
        try
        {
            var root = Path.GetPathRoot(Path.GetFullPath(_options.LogPath));
            if (root is not null)
            {
                var drive = new DriveInfo(root);
                diskFree = drive.AvailableFreeSpace;
                diskTotal = drive.TotalSize;
            }
        }
        catch
        {
        }

        return new FasterEventReaderWorkStoreDetailedMetrics(
            stateCounts,
            shardCounts,
            activeModelVersions,
            oldestAge,
            diskFree,
            diskTotal,
            _enqueueCount > 0 ? (double)_enqueueLatencyTicks / _enqueueCount / TimeSpan.TicksPerMillisecond : 0,
            _shardLeaseCount > 0 ? (double)_shardLeaseLatencyTicks / _shardLeaseCount / TimeSpan.TicksPerMillisecond : 0,
            _outputLeaseCount > 0 ? (double)_outputLeaseLatencyTicks / _outputLeaseCount / TimeSpan.TicksPerMillisecond : 0,
            _lastCheckpointTime);
    }

    public WorkStoreMetricsSnapshot GetWorkStoreMetrics()
    {
        var detailed = GetDetailedMetrics();
        return new WorkStoreMetricsSnapshot(
            detailed.BacklogByState,
            detailed.BacklogByShard,
            detailed.ActiveRuntimeModelVersions,
            detailed.OldestUnfinishedAge,
            detailed.DiskFreeBytes,
            detailed.DiskTotalBytes,
            detailed.AverageEnqueueLatencyMs,
            detailed.AverageShardLeaseLatencyMs,
            detailed.AverageOutputLeaseLatencyMs,
            detailed.LastCheckpointTime);
    }

    public IReadOnlySet<long> GetActiveRuntimeModelVersions()
    {
        return GetDetailedMetrics().ActiveRuntimeModelVersions;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_checkpointTimer is not null)
        {
            await _checkpointTimer.DisposeAsync().ConfigureAwait(false);
        }

        await TakeCheckpointAsync().ConfigureAwait(false);
        _session.Dispose();
        _store.Dispose();
        _log.Dispose();
        _objLog.Dispose();
        _gate.Dispose();
    }

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    private void RecoverOrBootstrap()
    {
        try
        {
            _store.Recover();
            RecoverStates().GetAwaiter().GetResult();
        }
        catch
        {
        }

        var seqKey = DurableSequenceWorkItemId;
        byte[]? seqOut = new byte[0];
        if (_session.Read(ref seqKey, ref seqOut).Found)
        {
            _nextWorkItemId = BitConverter.ToInt64(seqOut);
            _workItemIdLimit = _nextWorkItemId;
        }
        else
        {
            _nextWorkItemId = 1;
            _workItemIdLimit = 1;
        }

        var qSeqKey = DurableSequenceQueue;
        byte[]? qSeqOut = new byte[0];
        if (_session.Read(ref qSeqKey, ref qSeqOut).Found)
        {
            _nextQueueSequence = BitConverter.ToInt64(qSeqOut);
            _nextQueueLimit = _nextQueueSequence;
        }
        else
        {
            _nextQueueSequence = 1;
            _nextQueueLimit = 1;
        }
    }

    private async Task RecoverStates()
    {
        var prefix = MakeWorkItemPrefix();
        var now = DateTimeOffset.UtcNow;

        using var iter = _session.Iterate();
        while (iter.GetNext(out _))
        {
            var key = iter.GetKey();
            if (!key.StartsWith(prefix, StringComparison.Ordinal))
            {
                continue;
            }

            var record = await ReadJsonAsync<WorkItemRecord>(key).ConfigureAwait(false);
            if (record is null)
            {
                continue;
            }

            var targetState = record.State switch
            {
                (int)WorkState.Processing => (int)WorkState.Classified,
                (int)WorkState.Publishing => (int)WorkState.ReadyToOutput,
                _ => -1,
            };

            if (targetState == -1)
            {
                continue;
            }

            record.PreviousState = record.State;
            record.State = targetState;
            record.LeaseOwnerId = null;
            record.LeaseExpiresUtc = null;
            record.UpdatedUtc = now.ToString("O");
            await WriteJsonAsync(key, record).ConfigureAwait(false);
        }
    }

    private long AllocateWorkItemId()
    {
        if (_nextWorkItemId >= _workItemIdLimit)
        {
            var newLimit = _nextWorkItemId + WorkItemIdReserveBatch;
            var key = DurableSequenceWorkItemId;
            byte[] newLimitBytes = BitConverter.GetBytes(newLimit);
            _session.Upsert(ref key, ref newLimitBytes);
            _workItemIdLimit = newLimit;
        }

        return _nextWorkItemId++;
    }

    private long AllocateQueueSequence()
    {
        if (_nextQueueSequence >= _nextQueueLimit)
        {
            var newLimit = _nextQueueSequence + WorkItemIdReserveBatch;
            var key = DurableSequenceQueue;
            byte[] newLimitBytes = BitConverter.GetBytes(newLimit);
            _session.Upsert(ref key, ref newLimitBytes);
            _nextQueueLimit = newLimit;
        }

        return _nextQueueSequence++;
    }

    private void PersistDurableSequenceIfNeeded()
    {
        if (_nextWorkItemId < _workItemIdLimit)
        {
            return;
        }

        var key = DurableSequenceWorkItemId;
        byte[] limitBytes = BitConverter.GetBytes(_workItemIdLimit);
        _session.Upsert(ref key, ref limitBytes);
    }

    private void CheckpointIfNeeded()
    {
        if (_operationCount >= CheckpointAfterOperations)
        {
            _operationCount = 0;
            _lastCheckpointTime = DateTimeOffset.UtcNow;
#pragma warning disable CS4014
            _store.TakeFullCheckpointAsync(CheckpointType.FoldOver);
#pragma warning restore CS4014
        }
    }

    private async Task CheckpointLoopAsync()
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            await TakeCheckpointAsync().ConfigureAwait(false);
        }
        catch
        {
        }
    }

    private async Task TakeCheckpointAsync()
    {
        await Task.Run(async () =>
        {
            await _store.TakeFullCheckpointAsync(CheckpointType.FoldOver).ConfigureAwait(false);
        }).ConfigureAwait(false);
        _lastCheckpointTime = DateTimeOffset.UtcNow;
    }

    private async Task WriteSourceIndexAsync(string key, long workItemId)
    {
        byte[] value = BitConverter.GetBytes(workItemId);
        await _session.UpsertAsync(ref key, ref value).ConfigureAwait(false);
    }

    private async Task WriteJsonAsync<T>(string key, T record)
    {
        byte[] json = JsonSerializer.SerializeToUtf8Bytes(record, _jsonOptions);
        await _session.UpsertAsync(ref key, ref json).ConfigureAwait(false);
    }

    private async Task<T?> ReadJsonAsync<T>(string key)
    {
        byte[]? outValue = new byte[0];
        var result = await _session.ReadAsync(ref key, ref outValue).ConfigureAwait(false);
        if (!result.Status.Found || result.Output is null || result.Output.Length == 0)
        {
            return default;
        }

        return JsonSerializer.Deserialize<T>(result.Output, _jsonOptions);
    }

    private static QueueValue? ReadQueueValue(byte[]? value, JsonSerializerOptions jsonOptions)
    {
        if (value is null || value.Length == 0)
        {
            return null;
        }

        return JsonSerializer.Deserialize<QueueValue>(value, jsonOptions);
    }

    private static WorkItemRecord? ReadWorkItemRecord(byte[]? value, JsonSerializerOptions jsonOptions)
    {
        if (value is null || value.Length == 0)
        {
            return null;
        }

        return JsonSerializer.Deserialize<WorkItemRecord>(value, jsonOptions);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string MakeWorkItemKey(long workItemId) =>
        $"W:{workItemId:D20}";

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string MakeSourceIndexKey(KafkaSourceIdentity source) =>
        $"S:{source.SourceSystemId}|{source.Topic}|{source.Partition}|{source.Offset}";

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string MakeStateQueueKey(WorkState state, long sequence) =>
        $"Q:{(int)state:D2}|{sequence:D20}";

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string MakeShardQueueKey(int shardId, long sequence) =>
        $"H:{shardId:D10}|{sequence:D20}";

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string MakeOutputQueueKey(long sequence) =>
        $"O:{sequence:D20}";

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string MakeWorkItemPrefix() =>
        "W:";

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string MakeShardQueuePrefix(int shardId) =>
        $"H:{shardId:D10}|";

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string MakeOutputQueuePrefix() =>
        "O:";

    private static DateTimeOffset? ParseDateTimeOffset(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return null;
        }

        return DateTimeOffset.TryParse(value, out var result) ? result : null;
    }
}

public sealed record FasterEventReaderWorkStoreOptions
{
    public string LogPath { get; init; } = "data/eventreader/faster/work-log";
    public string CheckpointPath { get; init; } = "data/eventreader/faster/checkpoints";
    public int CheckpointIntervalMs { get; init; } = 60_000;
    public long IndexSizeBuckets { get; init; } = 1L << 20;
    public int LogPageSizeBits { get; init; } = 25;
    public int LogMemorySizeBits { get; init; } = 28;
}

public sealed record FasterEventReaderWorkStoreMetrics(
    long LastWorkItemId,
    long TotalOperations,
    string LogPath,
    string CheckpointPath);

public sealed record FasterEventReaderWorkStoreDetailedMetrics(
    IReadOnlyDictionary<WorkState, long> BacklogByState,
    IReadOnlyDictionary<int, long> BacklogByShard,
    IReadOnlySet<long> ActiveRuntimeModelVersions,
    TimeSpan? OldestUnfinishedAge,
    long DiskFreeBytes,
    long DiskTotalBytes,
    double AverageEnqueueLatencyMs,
    double AverageShardLeaseLatencyMs,
    double AverageOutputLeaseLatencyMs,
    DateTimeOffset? LastCheckpointTime);

internal sealed class QueueValue
{
    public long WorkItemId { get; set; }
}

internal sealed class WorkItemRecord
{
    public long WorkItemId { get; set; }
    public string SourceSystemId { get; set; } = "";
    public string Topic { get; set; } = "";
    public int Partition { get; set; }
    public long Offset { get; set; }
    public string? KafkaTimestampUtc { get; set; }
    public int FunctionId { get; set; }
    public long NedbankId { get; set; }
    public int ShardId { get; set; }
    public long RuntimeModelVersion { get; set; }
    public long FunctionVersion { get; set; }
    public long ExtractionScriptVersion { get; set; }
    public long RuleSetVersion { get; set; }
    public long OutputRouteVersion { get; set; }
    public int State { get; set; }
    public int PreviousState { get; set; } = -1;
    public int ProcessingAttempt { get; set; }
    public int OutputAttempt { get; set; }
    public string? RawPayload { get; set; }
    public string? OutputPayload { get; set; }
    public string? LeaseOwnerId { get; set; }
    public string? LeaseExpiresUtc { get; set; }
    public string CreatedUtc { get; set; } = "";
    public string UpdatedUtc { get; set; } = "";
    public string? LastError { get; set; }
    public string? CompletedUtc { get; set; }
}
