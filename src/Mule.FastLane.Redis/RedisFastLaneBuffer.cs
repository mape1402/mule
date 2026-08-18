namespace Mule.FastLane.Redis;

using System.Text.Json;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

internal sealed class RedisFastLaneBuffer
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
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
        var candidateValues = await database.SortedSetRangeByScoreAsync(
            PendingKey(lane),
            double.NegativeInfinity,
            Score(now),
            Exclude.None,
            Order.Ascending,
            0,
            Math.Max(1, batchSize) * 4);
        if (candidateValues.Length == 0)
            return Array.Empty<DurableAction>();

        var candidates = candidateValues
            .Select(value => Guid.TryParse((string)value, out var id) ? id : (Guid?)null)
            .Where(id => id != null)
            .Select(id => id.Value)
            .ToArray();
        if (candidates.Length == 0)
            return Array.Empty<DurableAction>();

        var token = Guid.NewGuid().ToString("N");
        var leaseBatch = database.CreateBatch();
        var leaseTasks = candidates
            .Select(id => (Id: id, Task: leaseBatch.StringSetAsync(GetLeaseKey(id), token, GetLeaseDuration(), When.NotExists)))
            .ToArray();
        leaseBatch.Execute();
        await Task.WhenAll(leaseTasks.Select(x => x.Task));

        var acquired = leaseTasks
            .Where(x => x.Task.Result)
            .Select(x => x.Id)
            .ToArray();
        if (acquired.Length == 0)
            return Array.Empty<DurableAction>();

        var readBatch = database.CreateBatch();
        var readTasks = acquired
            .Select(id => (Id: id, Task: readBatch.StringGetAsync(GetActionKey(id))))
            .ToArray();
        readBatch.Execute();
        await Task.WhenAll(readTasks.Select(x => x.Task));

        var envelopes = new List<RedisActionEnvelope>(Math.Min(batchSize, acquired.Length));
        var releaseIds = new List<Guid>();
        foreach (var item in readTasks)
        {
            if (envelopes.Count >= batchSize)
            {
                releaseIds.Add(item.Id);
                continue;
            }

            if (!item.Task.Result.HasValue)
            {
                releaseIds.Add(item.Id);
                continue;
            }

            var envelope = JsonSerializer.Deserialize<RedisActionEnvelope>((string)item.Task.Result, JsonOptions);
            if (envelope == null || !CanLock(envelope.ToAction(), lockTimeout, now))
            {
                releaseIds.Add(item.Id);
                continue;
            }

            envelope.Status = DurableActionStatus.Locked;
            envelope.LockedOnUtc = now;
            envelope.StartedOnUtc ??= now;
            envelope.IntentDirty = true;
            envelopes.Add(envelope);
        }

        if (envelopes.Count > 0)
        {
            var writeBatch = database.CreateBatch();
            var writeTasks = new List<Task>(envelopes.Count * 3);
            foreach (var envelope in envelopes)
            {
                writeTasks.Add(writeBatch.StringSetAsync(GetActionKey(envelope.Id), JsonSerializer.Serialize(envelope, JsonOptions)));
                writeTasks.Add(writeBatch.SortedSetRemoveAsync(PendingKey(NormalizeLane(envelope.Lane)), envelope.Id.ToString("D")));
                writeTasks.Add(writeBatch.SortedSetAddAsync(IntentFlushKey, envelope.Id.ToString("D"), Score(now)));
            }

            writeBatch.Execute();
            await Task.WhenAll(writeTasks);
        }

        if (releaseIds.Count > 0)
            await ReleaseLeasesAsync(releaseIds, token);

        return envelopes.Select(x => x.ToAction()).ToArray();
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
        var envelope = await GetEnvelopeAsync(id);
        if (envelope == null)
            return false;

        envelope.Status = DurableActionStatus.Completed;
        envelope.CompletedOnUtc = completedOnUtc;
        envelope.TerminalOnUtc = completedOnUtc;
        envelope.LockedOnUtc = null;
        envelope.NextAttemptOnUtc = null;
        envelope.LastError = null;
        envelope.TerminalDirty = true;
        await SaveEnvelopeAsync(envelope);
        await _connection.Database.SortedSetAddAsync(TerminalFlushKey, id.ToString("D"), Score(completedOnUtc));
        await ReleaseLeaseAsync(id);
        return true;
    }

    public async Task<bool> MarkFailedAsync(Guid id, string error, DateTimeOffset now, DateTimeOffset? nextAttemptOnUtc)
    {
        var envelope = await GetEnvelopeAsync(id);
        if (envelope == null)
            return false;

        envelope.Attempts++;
        envelope.Status = nextAttemptOnUtc == null ? DurableActionStatus.Failed : DurableActionStatus.Pending;
        envelope.LastError = error;
        envelope.LockedOnUtc = null;
        envelope.NextAttemptOnUtc = nextAttemptOnUtc;
        envelope.TerminalOnUtc = nextAttemptOnUtc == null ? now : null;
        envelope.IntentDirty = nextAttemptOnUtc != null;
        envelope.TerminalDirty = nextAttemptOnUtc == null;
        await SaveEnvelopeAsync(envelope);

        if (nextAttemptOnUtc == null)
            await _connection.Database.SortedSetAddAsync(TerminalFlushKey, id.ToString("D"), Score(now));
        else
        {
            await _connection.Database.SortedSetAddAsync(IntentFlushKey, id.ToString("D"), Score(now));
            await _connection.Database.SortedSetAddAsync(PendingKey(NormalizeLane(envelope.Lane)), id.ToString("D"), Score(nextAttemptOnUtc.Value));
        }

        await ReleaseLeaseAsync(id);
        return true;
    }

    public async Task<IReadOnlyCollection<DurableAction>> TakeIntentFlushBatchAsync(int batchSize)
    {
        var ids = await _connection.Database.SortedSetRangeByRankAsync(IntentFlushKey, 0, Math.Max(0, batchSize - 1));
        var actions = new List<DurableAction>(ids.Length);

        foreach (var value in ids)
        {
            if (!Guid.TryParse((string)value, out var id))
                continue;

            var envelope = await GetEnvelopeAsync(id);
            if (envelope is not { IntentDirty: true })
                continue;

            actions.Add(CloneForDurableIntent(envelope).ToAction());
        }

        return actions;
    }

    public async Task CompleteIntentFlushAsync(IReadOnlyCollection<Guid> ids)
    {
        foreach (var id in ids)
        {
            var envelope = await GetEnvelopeAsync(id);
            if (envelope == null)
                continue;

            envelope.IntentPersisted = true;
            envelope.IntentDirty = false;
            await SaveEnvelopeAsync(envelope);
            await _connection.Database.SortedSetRemoveAsync(IntentFlushKey, id.ToString("D"));
            await TryRemoveAsync(envelope);
        }
    }

    public async Task<IReadOnlyCollection<DurableAction>> TakeTerminalFlushBatchAsync(int batchSize)
    {
        var ids = await _connection.Database.SortedSetRangeByRankAsync(TerminalFlushKey, 0, Math.Max(0, batchSize - 1));
        var actions = new List<DurableAction>(ids.Length);

        foreach (var value in ids)
        {
            if (!Guid.TryParse((string)value, out var id))
                continue;

            var envelope = await GetEnvelopeAsync(id);
            if (envelope is not { IntentPersisted: true, TerminalDirty: true })
                continue;

            actions.Add(envelope.ToAction());
        }

        return actions;
    }

    public async Task CompleteTerminalFlushAsync(IReadOnlyCollection<Guid> ids)
    {
        foreach (var id in ids)
        {
            var envelope = await GetEnvelopeAsync(id);
            if (envelope == null)
                continue;

            envelope.TerminalDirty = false;
            envelope.TerminalFlushed = true;
            await SaveEnvelopeAsync(envelope);
            await _connection.Database.SortedSetRemoveAsync(TerminalFlushKey, id.ToString("D"));
            await TryRemoveAsync(envelope);
        }
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

    private static string NormalizeLane(string lane)
        => string.IsNullOrWhiteSpace(lane) ? MuleSettings.DefaultLane : lane;
}
