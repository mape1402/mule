namespace Mule.FastLane.Redis;

using System.Text.Json;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

internal sealed class RedisFastLaneBuffer
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private const string MarkCompletedScript = """
local payload = redis.call('GET', KEYS[1])
if not payload then
    return 0
end
local envelope = cjson.decode(payload)
envelope.status = tonumber(ARGV[1])
envelope.completedOnUtc = ARGV[2]
envelope.terminalOnUtc = ARGV[2]
envelope.lockedOnUtc = cjson.null
envelope.nextAttemptOnUtc = cjson.null
envelope.lastError = cjson.null
envelope.terminalDirty = true
redis.call('SET', KEYS[1], cjson.encode(envelope))
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
redis.call('DEL', KEYS[3])
return 1
""";

    private const string TakeIntentFlushScript = """
local ids = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[1]) * 4 - 1)
local claimed = {}
local count = 0
for _, id in ipairs(ids) do
    if count >= tonumber(ARGV[1]) then
        break
    end

    local payload = redis.call('GET', KEYS[2] .. id)
    if not payload then
        redis.call('ZREM', KEYS[1], id)
    else
        local envelope = cjson.decode(payload)
        if envelope.intentDirty == true and envelope.intentFlushing ~= true then
            envelope.intentFlushing = true
            local updated = cjson.encode(envelope)
            redis.call('SET', KEYS[2] .. id, updated)
            table.insert(claimed, updated)
            count = count + 1
        elseif envelope.intentDirty ~= true then
            redis.call('ZREM', KEYS[1], id)
        end
    end
end
return claimed
""";

    private const string TakeTerminalFlushScript = """
local ids = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[1]) * 4 - 1)
local claimed = {}
local count = 0
for _, id in ipairs(ids) do
    if count >= tonumber(ARGV[1]) then
        break
    end

    local payload = redis.call('GET', KEYS[2] .. id)
    if not payload then
        redis.call('ZREM', KEYS[1], id)
    else
        local envelope = cjson.decode(payload)
        if envelope.intentPersisted == true and envelope.terminalDirty == true and envelope.terminalFlushing ~= true then
            envelope.terminalFlushing = true
            local updated = cjson.encode(envelope)
            redis.call('SET', KEYS[2] .. id, updated)
            table.insert(claimed, updated)
            count = count + 1
        elseif envelope.terminalDirty ~= true then
            redis.call('ZREM', KEYS[1], id)
        end
    end
end
return claimed
""";

    private const string ClaimPendingScript = """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]) * 4)
local claimed = {}
local count = 0
for _, id in ipairs(ids) do
    if count >= tonumber(ARGV[2]) then
        break
    end

    local leaseAcquired = redis.call('SET', KEYS[4] .. id, ARGV[3], 'NX', 'PX', ARGV[4])
    if leaseAcquired then
        local payload = redis.call('GET', KEYS[3] .. id)
        if payload then
            local envelope = cjson.decode(payload)
            if envelope.status == tonumber(ARGV[5]) then
                envelope.status = tonumber(ARGV[6])
                envelope.lockedOnUtc = ARGV[7]
                if envelope.startedOnUtc == nil or envelope.startedOnUtc == cjson.null then
                    envelope.startedOnUtc = ARGV[7]
                end
                envelope.intentDirty = true
                local updated = cjson.encode(envelope)
                redis.call('SET', KEYS[3] .. id, updated)
                redis.call('ZREM', KEYS[1], id)
                redis.call('ZADD', KEYS[2], ARGV[1], id)
                table.insert(claimed, updated)
                count = count + 1
            else
                redis.call('DEL', KEYS[4] .. id)
            end
        else
            redis.call('DEL', KEYS[4] .. id)
            redis.call('ZREM', KEYS[1], id)
        end
    end
end
return claimed
""";

    private const string MarkFailedScript = """
local payload = redis.call('GET', KEYS[1])
if not payload then
    return 0
end
local envelope = cjson.decode(payload)
local retry = ARGV[5] == '1'
envelope.attempts = (envelope.attempts or 0) + 1
envelope.status = tonumber(ARGV[1])
envelope.lastError = ARGV[2]
envelope.lockedOnUtc = cjson.null
if retry then
    envelope.nextAttemptOnUtc = ARGV[6]
    envelope.terminalOnUtc = cjson.null
    envelope.intentDirty = true
    envelope.terminalDirty = false
    redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
    redis.call('ZADD', KEYS[4], ARGV[7], ARGV[4])
else
    envelope.nextAttemptOnUtc = cjson.null
    envelope.terminalOnUtc = ARGV[6]
    envelope.intentDirty = false
    envelope.terminalDirty = true
    redis.call('ZADD', KEYS[3], ARGV[3], ARGV[4])
end
redis.call('SET', KEYS[1], cjson.encode(envelope))
redis.call('DEL', KEYS[5])
return 1
""";

    private readonly RedisFastLaneConnection _connection;
    private readonly FastLaneRedisOptions _options;

    public RedisFastLaneBuffer(RedisFastLaneConnection connection, IOptions<FastLaneRedisOptions> options)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
    }

    public async Task<bool> TryAddAsync(DurableAction action)
    {
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        return (await TryAddRangeAsync([action])).Count == 1;
    }

    public async Task<IReadOnlyCollection<Guid>> TryAddRangeAsync(IReadOnlyCollection<DurableAction> actions)
    {
        if (actions == null)
            throw new ArgumentNullException(nameof(actions));

        if (actions.Count == 0)
            return Array.Empty<Guid>();

        var database = _connection.Database;
        var dedupeTasks = new List<(DurableAction Action, Task<bool> Task)>();
        var deduplicationRetention = GetDeduplicationRetention();
        var batch = database.CreateBatch();

        foreach (var action in actions)
        {
            if (action == null)
                throw new ArgumentException("Action collection cannot contain null values.", nameof(actions));

            if (string.IsNullOrWhiteSpace(action.DeduplicationKey))
                continue;

            var dedupeKey = DeduplicationKey(action.Key, action.DeduplicationKey);
            dedupeTasks.Add((
                action,
                batch.StringSetAsync(
                    dedupeKey,
                    action.Id.ToString("D"),
                    deduplicationRetention,
                    When.NotExists)));
        }

        batch.Execute();
        await Task.WhenAll(dedupeTasks.Select(x => x.Task));

        var dedupeResults = dedupeTasks.ToDictionary(x => x.Action.Id, x => x.Task.Result);
        var accepted = actions
            .Where(action => string.IsNullOrWhiteSpace(action.DeduplicationKey) ||
                             dedupeResults.GetValueOrDefault(action.Id))
            .ToArray();

        if (accepted.Length == 0)
            return Array.Empty<Guid>();

        batch = database.CreateBatch();
        var writeTasks = new List<Task>(accepted.Length * 4);
        foreach (var action in accepted)
        {
            var envelope = RedisActionEnvelope.FromAction(action);
            writeTasks.Add(batch.StringSetAsync(GetActionKey(envelope.Id), JsonSerializer.Serialize(envelope, JsonOptions)));
            writeTasks.Add(batch.SetAddAsync(LanesKey, NormalizeLane(envelope.Lane)));
            writeTasks.Add(batch.SortedSetAddAsync(IntentFlushKey, action.Id.ToString("D"), Score(action.CreatedOnUtc)));
            writeTasks.Add(batch.SortedSetAddAsync(PendingKey(NormalizeLane(action.Lane)), action.Id.ToString("D"), Score(action.NextAttemptOnUtc ?? DateTimeOffset.UtcNow)));
        }

        batch.Execute();
        await Task.WhenAll(writeTasks);
        return accepted.Select(x => x.Id).ToArray();
    }

    public async Task<Guid?> FindByDeduplicationKeyAsync(ActionKey key, string deduplicationKey)
    {
        if (string.IsNullOrWhiteSpace(deduplicationKey))
            return null;

        var value = await _connection.Database.StringGetAsync(DeduplicationKey(key, deduplicationKey));
        return Guid.TryParse((string)value, out var id) ? id : null;
    }

    public async Task<DurableAction> LockAsync(Guid id, TimeSpan lockTimeout, DateTimeOffset now)
    {
        var token = Guid.NewGuid().ToString("N");
        if (!await TryAcquireLeaseAsync(id, token))
            return null;

        var envelope = await GetEnvelopeAsync(id);
        if (envelope == null || !CanLock(envelope.ToAction(), lockTimeout, now))
        {
            await ReleaseLeaseAsync(id, token);
            return null;
        }

        envelope.Status = DurableActionStatus.Locked;
        envelope.LockedOnUtc = now;
        envelope.StartedOnUtc ??= now;
        envelope.IntentDirty = true;
        await SaveEnvelopeAsync(envelope);
        await _connection.Database.SortedSetRemoveAsync(PendingKey(NormalizeLane(envelope.Lane)), id.ToString("D"));
        await _connection.Database.SortedSetAddAsync(IntentFlushKey, id.ToString("D"), Score(now));
        return envelope.ToAction();
    }

    public async Task<IReadOnlyCollection<DurableAction>> ClaimPendingAsync(
        string lane,
        int batchSize,
        TimeSpan lockTimeout,
        DateTimeOffset now)
    {
        lane = NormalizeLane(lane);
        var database = _connection.Database;
        var token = Guid.NewGuid().ToString("N");
        var result = await database.ScriptEvaluateAsync(
            ClaimPendingScript,
            [
                PendingKey(lane),
                IntentFlushKey,
                $"{_connection.Prefix}:fastlane:action:",
                $"{_connection.Prefix}:fastlane:lease:"
            ],
            [
                Score(now),
                Math.Max(1, batchSize),
                token,
                Math.Max(1, (long)GetLeaseDuration().TotalMilliseconds),
                (int)DurableActionStatus.Pending,
                (int)DurableActionStatus.Locked,
                FormatDate(now)
            ]);
        var values = (RedisResult[])result;

        return values
            .Select(value => JsonSerializer.Deserialize<RedisActionEnvelope>((string)(RedisValue)value, JsonOptions))
            .Where(envelope => envelope != null)
            .Select(envelope => envelope.ToAction())
            .ToArray();
    }

    public async Task<DateTimeOffset?> GetNextPendingOnUtcAsync()
    {
        var lanes = await _connection.Database.SetMembersAsync(LanesKey);
        DateTimeOffset? next = null;

        foreach (var lane in lanes)
        {
            var values = await _connection.Database.SortedSetRangeByRankWithScoresAsync(
                PendingKey((string)lane),
                0,
                0,
                Order.Ascending);
            var score = values.FirstOrDefault().Score;
            if (score <= 0)
                continue;

            var due = FromScore(score);
            next = next == null || due < next ? due : next;
        }

        return next;
    }

    public async Task<bool> MarkCompletedAsync(Guid id, DateTimeOffset completedOnUtc)
    {
        var database = _connection.Database;
        var result = await database.ScriptEvaluateAsync(
            MarkCompletedScript,
            [
                GetActionKey(id),
                TerminalFlushKey,
                GetLeaseKey(id)
            ],
            [
                (int)DurableActionStatus.Completed,
                FormatDate(completedOnUtc),
                Score(completedOnUtc),
                id.ToString("D")
            ]);
        return (int)result == 1;
    }

    public async Task<bool> MarkFailedAsync(Guid id, string error, DateTimeOffset now, DateTimeOffset? nextAttemptOnUtc)
    {
        var envelope = await GetEnvelopeAsync(id);
        if (envelope == null)
            return false;

        var database = _connection.Database;
        var status = nextAttemptOnUtc == null ? DurableActionStatus.Failed : DurableActionStatus.Pending;
        var result = await database.ScriptEvaluateAsync(
            MarkFailedScript,
            [
                GetActionKey(id),
                IntentFlushKey,
                TerminalFlushKey,
                PendingKey(NormalizeLane(envelope.Lane)),
                GetLeaseKey(id)
            ],
            [
                (int)status,
                error ?? string.Empty,
                Score(now),
                id.ToString("D"),
                nextAttemptOnUtc == null ? "0" : "1",
                FormatDate(nextAttemptOnUtc ?? now),
                nextAttemptOnUtc == null ? 0 : Score(nextAttemptOnUtc.Value)
            ]);
        return (int)result == 1;
    }

    public async Task<IReadOnlyCollection<DurableAction>> TakeIntentFlushBatchAsync(int batchSize)
    {
        var database = _connection.Database;
        var result = await database.ScriptEvaluateAsync(
            TakeIntentFlushScript,
            [IntentFlushKey, $"{_connection.Prefix}:fastlane:action:"],
            [Math.Max(1, batchSize)]);
        var values = (RedisResult[])result;

        return values
            .Select(value => JsonSerializer.Deserialize<RedisActionEnvelope>((string)(RedisValue)value, JsonOptions))
            .Where(envelope => envelope != null)
            .Select(envelope => CloneForDurableIntent(envelope).ToAction())
            .ToArray();
    }

    public async Task CompleteIntentFlushAsync(IReadOnlyCollection<Guid> ids)
    {
        if (ids.Count == 0)
            return;

        var database = _connection.Database;
        var readBatch = database.CreateBatch();
        var readTasks = ids
            .Select(id => (Id: id, Task: readBatch.StringGetAsync(GetActionKey(id))))
            .ToArray();
        readBatch.Execute();
        await Task.WhenAll(readTasks.Select(x => x.Task));

        var writeBatch = database.CreateBatch();
        var writeTasks = new List<Task>(ids.Count * 3);
        foreach (var item in readTasks)
        {
            if (!item.Task.Result.HasValue)
                continue;

            var envelope = JsonSerializer.Deserialize<RedisActionEnvelope>((string)item.Task.Result, JsonOptions);
            if (envelope == null)
                continue;

            envelope.IntentPersisted = true;
            envelope.IntentDirty = false;
            envelope.IntentFlushing = false;
            AddSaveEnvelope(writeBatch, writeTasks, envelope);
            writeTasks.Add(writeBatch.SortedSetRemoveAsync(IntentFlushKey, item.Id.ToString("D")));
            AddTryRemove(writeBatch, writeTasks, envelope);
        }

        writeBatch.Execute();
        await Task.WhenAll(writeTasks);
    }

    public async Task CancelIntentFlushAsync(IReadOnlyCollection<Guid> ids)
    {
        if (ids.Count == 0)
            return;

        var database = _connection.Database;
        var readBatch = database.CreateBatch();
        var readTasks = ids
            .Select(id => (Id: id, Task: readBatch.StringGetAsync(GetActionKey(id))))
            .ToArray();
        readBatch.Execute();
        await Task.WhenAll(readTasks.Select(x => x.Task));

        var writeBatch = database.CreateBatch();
        var writeTasks = new List<Task>(ids.Count);
        foreach (var item in readTasks)
        {
            if (!item.Task.Result.HasValue)
                continue;

            var envelope = JsonSerializer.Deserialize<RedisActionEnvelope>((string)item.Task.Result, JsonOptions);
            if (envelope == null)
                continue;

            envelope.IntentFlushing = false;
            AddSaveEnvelope(writeBatch, writeTasks, envelope);
        }

        writeBatch.Execute();
        await Task.WhenAll(writeTasks);
    }

    public async Task<IReadOnlyCollection<DurableAction>> TakeTerminalFlushBatchAsync(int batchSize)
    {
        var database = _connection.Database;
        var result = await database.ScriptEvaluateAsync(
            TakeTerminalFlushScript,
            [TerminalFlushKey, $"{_connection.Prefix}:fastlane:action:"],
            [Math.Max(1, batchSize)]);
        var values = (RedisResult[])result;

        return values
            .Select(value => JsonSerializer.Deserialize<RedisActionEnvelope>((string)(RedisValue)value, JsonOptions))
            .Where(envelope => envelope != null)
            .Select(envelope => envelope.ToAction())
            .ToArray();
    }

    public async Task CompleteTerminalFlushAsync(IReadOnlyCollection<Guid> ids)
    {
        if (ids.Count == 0)
            return;

        var database = _connection.Database;
        var readBatch = database.CreateBatch();
        var readTasks = ids
            .Select(id => (Id: id, Task: readBatch.StringGetAsync(GetActionKey(id))))
            .ToArray();
        readBatch.Execute();
        await Task.WhenAll(readTasks.Select(x => x.Task));

        var writeBatch = database.CreateBatch();
        var writeTasks = new List<Task>(ids.Count * 3);
        foreach (var item in readTasks)
        {
            if (!item.Task.Result.HasValue)
                continue;

            var envelope = JsonSerializer.Deserialize<RedisActionEnvelope>((string)item.Task.Result, JsonOptions);
            if (envelope == null)
                continue;

            envelope.TerminalDirty = false;
            envelope.TerminalFlushed = true;
            envelope.TerminalFlushing = false;
            AddSaveEnvelope(writeBatch, writeTasks, envelope);
            writeTasks.Add(writeBatch.SortedSetRemoveAsync(TerminalFlushKey, item.Id.ToString("D")));
            AddTryRemove(writeBatch, writeTasks, envelope);
        }

        writeBatch.Execute();
        await Task.WhenAll(writeTasks);
    }

    public async Task CancelTerminalFlushAsync(IReadOnlyCollection<Guid> ids)
    {
        if (ids.Count == 0)
            return;

        var database = _connection.Database;
        var readBatch = database.CreateBatch();
        var readTasks = ids
            .Select(id => (Id: id, Task: readBatch.StringGetAsync(GetActionKey(id))))
            .ToArray();
        readBatch.Execute();
        await Task.WhenAll(readTasks.Select(x => x.Task));

        var writeBatch = database.CreateBatch();
        var writeTasks = new List<Task>(ids.Count);
        foreach (var item in readTasks)
        {
            if (!item.Task.Result.HasValue)
                continue;

            var envelope = JsonSerializer.Deserialize<RedisActionEnvelope>((string)item.Task.Result, JsonOptions);
            if (envelope == null)
                continue;

            envelope.TerminalFlushing = false;
            AddSaveEnvelope(writeBatch, writeTasks, envelope);
        }

        writeBatch.Execute();
        await Task.WhenAll(writeTasks);
    }

    private async Task SaveEnvelopeAsync(RedisActionEnvelope envelope)
    {
        await _connection.Database.StringSetAsync(GetActionKey(envelope.Id), JsonSerializer.Serialize(envelope, JsonOptions));
        await _connection.Database.SetAddAsync(LanesKey, NormalizeLane(envelope.Lane));
    }

    private async Task<RedisActionEnvelope> GetEnvelopeAsync(Guid id)
    {
        var value = await _connection.Database.StringGetAsync(GetActionKey(id));
        return value.HasValue
            ? JsonSerializer.Deserialize<RedisActionEnvelope>((string)value, JsonOptions)
            : null;
    }

    private async Task<bool> TryAcquireLeaseAsync(Guid id, string token)
        => await _connection.Database.StringSetAsync(GetLeaseKey(id), token, GetLeaseDuration(), When.NotExists);

    private async Task ReleaseLeaseAsync(Guid id, string token = null)
    {
        if (token == null)
        {
            await _connection.Database.KeyDeleteAsync(GetLeaseKey(id));
            return;
        }

        var value = await _connection.Database.StringGetAsync(GetLeaseKey(id));
        if (value == token)
            await _connection.Database.KeyDeleteAsync(GetLeaseKey(id));
    }

    private async Task ReleaseLeasesAsync(IReadOnlyCollection<Guid> ids, string token)
    {
        foreach (var id in ids)
            await ReleaseLeaseAsync(id, token);
    }

    private async Task TryRemoveAsync(RedisActionEnvelope envelope)
    {
        if (!envelope.IntentPersisted || envelope.IntentDirty || !envelope.TerminalFlushed || envelope.TerminalDirty)
            return;

        await _connection.Database.KeyDeleteAsync(GetActionKey(envelope.Id));
        if (!string.IsNullOrWhiteSpace(envelope.DeduplicationKey))
            await _connection.Database.KeyDeleteAsync(DeduplicationKey(ActionKey.From(envelope.Key), envelope.DeduplicationKey));
    }

    private void AddSaveEnvelope(IBatch batch, List<Task> tasks, RedisActionEnvelope envelope)
    {
        tasks.Add(batch.StringSetAsync(GetActionKey(envelope.Id), JsonSerializer.Serialize(envelope, JsonOptions)));
        tasks.Add(batch.SetAddAsync(LanesKey, NormalizeLane(envelope.Lane)));
    }

    private void AddTryRemove(IBatch batch, List<Task> tasks, RedisActionEnvelope envelope)
    {
        if (!envelope.IntentPersisted || envelope.IntentDirty || !envelope.TerminalFlushed || envelope.TerminalDirty)
            return;

        tasks.Add(batch.KeyDeleteAsync(GetActionKey(envelope.Id)));
        if (!string.IsNullOrWhiteSpace(envelope.DeduplicationKey))
            tasks.Add(batch.KeyDeleteAsync(DeduplicationKey(ActionKey.From(envelope.Key), envelope.DeduplicationKey)));
    }

    private RedisActionEnvelope CloneForDurableIntent(RedisActionEnvelope envelope)
    {
        var clone = RedisActionEnvelope.FromAction(envelope.ToAction());
        if (clone.Status is DurableActionStatus.Pending or DurableActionStatus.Completed or DurableActionStatus.Failed)
        {
            clone.Status = DurableActionStatus.Locked;
            clone.LockedOnUtc ??= DateTimeOffset.UtcNow;
            if (envelope.Status == DurableActionStatus.Failed)
                clone.Attempts = Math.Max(0, envelope.Attempts - 1);
            clone.CompletedOnUtc = null;
            clone.TerminalOnUtc = null;
            clone.LastError = null;
        }

        return clone;
    }

    private static bool CanLock(DurableAction action, TimeSpan lockTimeout, DateTimeOffset now)
    {
        var lockExpiration = now.Subtract(lockTimeout);
        return action.Status == DurableActionStatus.Pending && (action.NextAttemptOnUtc == null || action.NextAttemptOnUtc <= now) ||
               action.Status == DurableActionStatus.Locked && action.LockedOnUtc <= lockExpiration;
    }

    private string GetActionKey(Guid id) => $"{_connection.Prefix}:fastlane:action:{id:D}";

    private string GetLeaseKey(Guid id) => $"{_connection.Prefix}:fastlane:lease:{id:D}";

    private string DeduplicationKey(ActionKey key, string deduplicationKey)
        => $"{_connection.Prefix}:fastlane:dedupe:{key.Value}:{deduplicationKey}";

    private string PendingKey(string lane) => $"{_connection.Prefix}:fastlane:pending:{NormalizeLane(lane)}";

    private string IntentFlushKey => $"{_connection.Prefix}:fastlane:intent-flush";

    private string TerminalFlushKey => $"{_connection.Prefix}:fastlane:terminal-flush";

    private string LanesKey => $"{_connection.Prefix}:fastlane:lanes";

    private TimeSpan GetLeaseDuration()
        => _options.LeaseDuration <= TimeSpan.Zero ? TimeSpan.FromMinutes(2) : _options.LeaseDuration;

    private TimeSpan GetDeduplicationRetention()
        => _options.DeduplicationRetention <= TimeSpan.Zero ? TimeSpan.FromDays(7) : _options.DeduplicationRetention;

    private static double Score(DateTimeOffset value)
        => value.ToUnixTimeMilliseconds();

    private static DateTimeOffset FromScore(double score)
        => DateTimeOffset.FromUnixTimeMilliseconds((long)score);

    private static string FormatDate(DateTimeOffset value)
        => value.ToUniversalTime().ToString("O");

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;
}
