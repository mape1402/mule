namespace Mule.Redis;

using Microsoft.Extensions.Options;
using Mule.Diagnostics;
using StackExchange.Redis;

internal sealed class RedisMuleDiagnostics : IMuleDiagnostics
{
    private readonly RedisMuleConnection _connection;
    private readonly MuleRuntimeMetrics _metrics;
    private readonly TimeSpan _lockTimeout;

    public RedisMuleDiagnostics(
        RedisMuleConnection connection,
        MuleRuntimeMetrics metrics,
        IOptions<MuleSettings> settings)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        var configured = settings?.Value ?? throw new ArgumentNullException(nameof(settings));
        _lockTimeout = configured.LockTimeout <= TimeSpan.Zero ? TimeSpan.FromMinutes(5) : configured.LockTimeout;
    }

    public async Task<MuleDiagnosticsSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var database = await _connection.GetDatabaseAsync(cancellationToken);
        var keys = _connection.Keyspace;
        var now = DateTimeOffset.UtcNow;
        var nowMs = now.ToUnixTimeMilliseconds();
        var runtime = _metrics.GetSnapshot(now);

        var pendingTask = database.SortedSetLengthAsync(keys.Pending);
        var lockedTask = database.SortedSetLengthAsync(keys.Locked);
        var completedTask = database.SortedSetLengthAsync(keys.Completed);
        var failedTask = database.SortedSetLengthAsync(keys.Failed);
        var cancelledTask = database.SortedSetLengthAsync(keys.Cancelled);
        var expiredTask = database.SortedSetLengthAsync(
            keys.Locked,
            double.NegativeInfinity,
            now.Subtract(_lockTimeout).ToUnixTimeMilliseconds());
        var throughputTask = database.SortedSetLengthAsync(
            keys.Completed,
            now.AddMinutes(-1).ToUnixTimeMilliseconds(),
            nowMs);
        var pendingOldestTask = OldestAsync(database, keys.Pending);
        var lockedOldestTask = OldestAsync(database, keys.Locked);
        var failedOldestTask = OldestAsync(database, keys.Failed);
        var backlogLaneTask = database.HashGetAllAsync(keys.BacklogByLane);
        var backlogActionTask = database.HashGetAllAsync(keys.BacklogByActionKey);
        var failuresTask = database.HashGetAllAsync(keys.FailuresByActionKey);
        var retriesTask = database.HashGetAllAsync(keys.RetriesByActionKey);
        var latenciesTask = database.HashGetAllAsync(keys.Latencies);

        await Task.WhenAll(
            pendingTask, lockedTask, completedTask, failedTask, cancelledTask,
            expiredTask, throughputTask, pendingOldestTask, lockedOldestTask, failedOldestTask,
            backlogLaneTask, backlogActionTask, failuresTask, retriesTask, latenciesTask);
        cancellationToken.ThrowIfCancellationRequested();

        var oldestPending = pendingOldestTask.Result;
        var oldestLocked = lockedOldestTask.Result;
        var latencies = latenciesTask.Result.ToDictionary(x => x.Name.ToString(), x => x.Value, StringComparer.Ordinal);
        return new MuleDiagnosticsSnapshot
        {
            Pending = checked((int)pendingTask.Result),
            Locked = checked((int)lockedTask.Result),
            Completed = checked((int)completedTask.Result),
            Failed = checked((int)failedTask.Result),
            Cancelled = checked((int)cancelledTask.Result),
            LockExpired = checked((int)expiredTask.Result),
            DuplicatesIgnored = runtime.DuplicatesIgnored,
            ThroughputPerMinute = Math.Max(runtime.ThroughputPerMinute, checked((int)throughputTask.Result)),
            RuntimeCompleted = runtime.Completed,
            RuntimeFailed = runtime.Failed,
            ReplayScheduled = runtime.ReplayScheduled,
            ReplayMaterialized = runtime.ReplayMaterialized,
            ReplayAlreadyScheduled = runtime.ReplayAlreadyScheduled,
            ReplayRejected = runtime.ReplayRejected,
            ReplayRetried = runtime.ReplayRetried,
            ReplayMaterializationFailures = runtime.ReplayMaterializationFailures,
            OldestPendingOnUtc = oldestPending,
            OldestLockedOnUtc = oldestLocked,
            OldestFailedOnUtc = failedOldestTask.Result,
            OldestPendingAge = oldestPending.HasValue ? now - oldestPending.Value : null,
            OldestLockedAge = oldestLocked.HasValue ? now - oldestLocked.Value : null,
            AverageEnqueueToExecutionLatency = Average(latencies, "enqueueExecutionSum", "enqueueExecutionCount"),
            AverageExecutionLatency = Average(latencies, "executionSum", "executionCount"),
            AverageEnqueueToTerminalLatency = Average(latencies, "terminalSum", "terminalCount"),
            BacklogByLane = StringCounts(backlogLaneTask.Result),
            BacklogByActionKey = ActionCounts(backlogActionTask.Result),
            FailuresByActionKey = ActionCounts(failuresTask.Result),
            RetriesByActionKey = ActionCounts(retriesTask.Result),
            CompletedPerMinuteByLane = runtime.CompletedPerMinuteByLane,
            CompletedPerMinuteByActionKey = runtime.CompletedPerMinuteByActionKey,
            RuntimeFailedByLane = runtime.RuntimeFailedByLane,
            RuntimeFailedByActionKey = runtime.RuntimeFailedByActionKey,
            ClaimedByLane = runtime.ClaimedByLane,
            WaitingExecutionByLane = runtime.WaitingExecutionByLane,
            ExecutingByLane = runtime.ExecutingByLane,
            ExecutorSaturationByLane = runtime.ExecutorSaturationByLane,
            OldestWaitingExecutionAgeByLane = runtime.OldestWaitingExecutionAgeByLane
        };
    }

    private static async Task<DateTimeOffset?> OldestAsync(IDatabase database, RedisKey key)
    {
        var entries = await database.SortedSetRangeByRankWithScoresAsync(key, 0, 0, Order.Ascending);
        return entries.Length == 0
            ? null
            : DateTimeOffset.FromUnixTimeMilliseconds((long)entries[0].Score);
    }

    private static IReadOnlyDictionary<string, int> StringCounts(HashEntry[] entries)
        => entries
            .Select(x => (Key: x.Name.ToString(), Count: ParseCount(x.Value)))
            .Where(x => x.Count > 0)
            .ToDictionary(x => x.Key, x => x.Count, StringComparer.OrdinalIgnoreCase);

    private static IReadOnlyDictionary<ActionKey, int> ActionCounts(HashEntry[] entries)
        => entries
            .Select(x => (Key: ActionKey.From(x.Name.ToString()), Count: ParseCount(x.Value)))
            .Where(x => x.Count > 0)
            .ToDictionary(x => x.Key, x => x.Count);

    private static int ParseCount(RedisValue value)
        => int.TryParse(value.ToString(), out var parsed) ? parsed : 0;

    private static TimeSpan? Average(
        IReadOnlyDictionary<string, RedisValue> values,
        string sumName,
        string countName)
    {
        if (!values.TryGetValue(sumName, out var sumValue)
            || !values.TryGetValue(countName, out var countValue)
            || !double.TryParse(sumValue.ToString(), System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var sum)
            || !long.TryParse(countValue.ToString(), out var count)
            || count <= 0)
            return null;

        return TimeSpan.FromMilliseconds(sum / count);
    }
}
