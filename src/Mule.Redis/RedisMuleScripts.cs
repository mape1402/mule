namespace Mule.Redis;

internal static class RedisMuleScripts
{
    public const string AddActions = """
local prefix = ARGV[1]
local count = tonumber(ARGV[2])
local width = 22
local added = 0
local duplicates = 0

for index = 0, count - 1 do
    local offset = 3 + index * width
    local id = ARGV[offset]
    local actionKey = prefix .. ':action:' .. id
    local dedupeField = ARGV[offset + 10]
    local canAdd = redis.call('EXISTS', actionKey) == 0

    if canAdd and dedupeField ~= '' then
        local existing = redis.call('HGET', KEYS[1], dedupeField)
        if existing then
            canAdd = false
            duplicates = duplicates + 1
        else
            redis.call('HSET', KEYS[1], dedupeField, id)
        end
    end

    if canAdd then
        local status = tonumber(ARGV[offset + 11])
        local created = tonumber(ARGV[offset + 14])
        local locked = tonumber(ARGV[offset + 15]) or created
        local completed = tonumber(ARGV[offset + 18]) or created
        local cancelled = tonumber(ARGV[offset + 19]) or created
        local terminal = tonumber(ARGV[offset + 20]) or created
        local due = tonumber(ARGV[offset + 21])
        local lane = ARGV[offset + 3]
        local laneDue = ARGV[offset + 4]
        local actionType = ARGV[offset + 2]

        redis.call('HSET', actionKey,
            'id', id,
            'ct', ARGV[offset + 1],
            'key', actionType,
            'lane', lane,
            'laneDue', laneDue,
            'payload', ARGV[offset + 5],
            'payloadType', ARGV[offset + 6],
            'metadata', ARGV[offset + 7],
            'correlation', ARGV[offset + 8],
            'dedupe', ARGV[offset + 9],
            'dedupeField', dedupeField,
            'status', status,
            'attempts', ARGV[offset + 12],
            'error', ARGV[offset + 13],
            'created', ARGV[offset + 14],
            'locked', ARGV[offset + 15],
            'started', ARGV[offset + 16],
            'next', ARGV[offset + 17],
            'completed', ARGV[offset + 18],
            'cancelled', ARGV[offset + 19],
            'terminal', ARGV[offset + 20])

        redis.call('SADD', KEYS[3], lane)
        if status == 0 or status == 1 then
            redis.call('ZADD', KEYS[2], due, id)
            redis.call('ZADD', laneDue, due, id)
            redis.call('HINCRBY', KEYS[9], lane, 1)
            redis.call('HINCRBY', KEYS[10], actionType, 1)
        end

        local score = created
        if status == 1 then score = locked end
        if status == 2 then score = completed end
        if status == 3 then score = terminal end
        if status == 4 then score = cancelled end
        redis.call('ZADD', KEYS[4 + status], score, id)
        added = added + 1
    end
end

return { added, duplicates }
""";

    public const string ClaimPending = """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]) * 4)
local claimed = {}
local count = 0
local now = tonumber(ARGV[1])
local lockTimeout = tonumber(ARGV[3])

for _, id in ipairs(ids) do
    if count >= tonumber(ARGV[2]) then break end
    local actionKey = ARGV[4] .. ':action:' .. id
    if redis.call('EXISTS', actionKey) == 0 then
        redis.call('ZREM', KEYS[1], id)
        redis.call('ZREM', KEYS[2], id)
    else
        local status = tonumber(redis.call('HGET', actionKey, 'status'))
        local locked = tonumber(redis.call('HGET', actionKey, 'locked'))
        local nextAttempt = tonumber(redis.call('HGET', actionKey, 'next'))
        local canClaim = status == 0 and (not nextAttempt or nextAttempt <= now)
        if status == 1 and locked and locked + lockTimeout <= now then canClaim = true end

        if canClaim then
            local started = redis.call('HGET', actionKey, 'started')
            local created = tonumber(redis.call('HGET', actionKey, 'created'))
            local token = ARGV[5 + count]
            redis.call('HSET', actionKey,
                'status', 1,
                'locked', now,
                'started', (started and started ~= '') and started or now,
                'ct', token)
            if not started or started == '' then
                redis.call('HINCRBYFLOAT', KEYS[5], 'enqueueExecutionSum', now - created)
                redis.call('HINCRBY', KEYS[5], 'enqueueExecutionCount', 1)
            end
            redis.call('ZREM', KEYS[3], id)
            redis.call('ZADD', KEYS[4], now, id)
            redis.call('ZADD', KEYS[1], now + lockTimeout, id)
            redis.call('ZADD', KEYS[2], now + lockTimeout, id)
            table.insert(claimed, id)
            count = count + 1
        end
    end
end

return claimed
""";

    public const string LockAction = """
local actionKey = KEYS[1]
if redis.call('EXISTS', actionKey) == 0 then return 0 end
local status = tonumber(redis.call('HGET', actionKey, 'status'))
local locked = tonumber(redis.call('HGET', actionKey, 'locked'))
local nextAttempt = tonumber(redis.call('HGET', actionKey, 'next'))
local now = tonumber(ARGV[1])
local timeout = tonumber(ARGV[2])
local canLock = status == 0 and (not nextAttempt or nextAttempt <= now)
if status == 1 and locked and locked + timeout <= now then canLock = true end
if not canLock then return 0 end

local id = redis.call('HGET', actionKey, 'id')
local laneDue = redis.call('HGET', actionKey, 'laneDue')
local started = redis.call('HGET', actionKey, 'started')
local created = tonumber(redis.call('HGET', actionKey, 'created'))
redis.call('HSET', actionKey,
    'status', 1,
    'locked', now,
    'started', (started and started ~= '') and started or now,
    'ct', ARGV[3])
if not started or started == '' then
    redis.call('HINCRBYFLOAT', KEYS[6], 'enqueueExecutionSum', now - created)
    redis.call('HINCRBY', KEYS[6], 'enqueueExecutionCount', 1)
end
redis.call('ZREM', KEYS[4], id)
redis.call('ZADD', KEYS[5], now, id)
redis.call('ZADD', KEYS[2], now + timeout, id)
redis.call('ZADD', laneDue, now + timeout, id)
return 1
""";

    public const string MarkCompleted = """
local count = tonumber(ARGV[1])
local changed = 0
for index = 0, count - 1 do
    local id = ARGV[2 + index * 3]
    local completed = tonumber(ARGV[3 + index * 3])
    local token = ARGV[4 + index * 3]
    local actionKey = ARGV[2 + count * 3] .. ':action:' .. id
    if redis.call('EXISTS', actionKey) == 1 then
        local status = tonumber(redis.call('HGET', actionKey, 'status'))
        if status == 0 or status == 1 then
            local lane = redis.call('HGET', actionKey, 'lane')
            local laneDue = redis.call('HGET', actionKey, 'laneDue')
            local actionType = redis.call('HGET', actionKey, 'key')
            local created = tonumber(redis.call('HGET', actionKey, 'created'))
            local started = tonumber(redis.call('HGET', actionKey, 'started'))
            redis.call('HSET', actionKey,
                'status', 2, 'completed', completed, 'terminal', completed,
                'locked', '', 'next', '', 'error', '', 'ct', token)
            redis.call('ZREM', KEYS[1], id)
            redis.call('ZREM', laneDue, id)
            redis.call('ZREM', KEYS[2], id)
            redis.call('ZREM', KEYS[3], id)
            redis.call('ZADD', KEYS[4], completed, id)
            redis.call('HINCRBY', KEYS[5], lane, -1)
            redis.call('HINCRBY', KEYS[6], actionType, -1)
            if started then
                redis.call('HINCRBYFLOAT', KEYS[7], 'executionSum', completed - started)
                redis.call('HINCRBY', KEYS[7], 'executionCount', 1)
            end
            redis.call('HINCRBYFLOAT', KEYS[7], 'terminalSum', completed - created)
            redis.call('HINCRBY', KEYS[7], 'terminalCount', 1)
            changed = changed + 1
        end
    end
end
return changed
""";

    public const string MarkFailed = """
local actionKey = KEYS[1]
if redis.call('EXISTS', actionKey) == 0 then return 0 end
local status = tonumber(redis.call('HGET', actionKey, 'status'))
if status ~= 0 and status ~= 1 then return 0 end
local id = redis.call('HGET', actionKey, 'id')
local lane = redis.call('HGET', actionKey, 'lane')
local laneDue = redis.call('HGET', actionKey, 'laneDue')
local actionType = redis.call('HGET', actionKey, 'key')
local created = tonumber(redis.call('HGET', actionKey, 'created'))
local attempts = tonumber(redis.call('HGET', actionKey, 'attempts') or '0') + 1
local now = tonumber(ARGV[1])
local nextAttempt = tonumber(ARGV[3])
redis.call('HINCRBY', KEYS[8], actionType, 1)

redis.call('ZREM', KEYS[4], id)
redis.call('ZREM', KEYS[5], id)
if nextAttempt then
    redis.call('HSET', actionKey,
        'status', 0, 'attempts', attempts, 'error', ARGV[2],
        'locked', '', 'next', nextAttempt, 'terminal', '', 'ct', ARGV[4])
    redis.call('ZADD', KEYS[4], created, id)
    redis.call('ZADD', KEYS[2], nextAttempt, id)
    redis.call('ZADD', laneDue, nextAttempt, id)
else
    redis.call('HSET', actionKey,
        'status', 3, 'attempts', attempts, 'error', ARGV[2],
        'locked', '', 'next', '', 'terminal', now, 'ct', ARGV[4])
    redis.call('ZREM', KEYS[2], id)
    redis.call('ZREM', laneDue, id)
    redis.call('ZADD', KEYS[6], created, id)
    redis.call('HINCRBY', KEYS[3], lane, -1)
    redis.call('HINCRBY', KEYS[7], actionType, -1)
    redis.call('HINCRBY', KEYS[9], actionType, 1)
    redis.call('HINCRBYFLOAT', KEYS[10], 'terminalSum', now - created)
    redis.call('HINCRBY', KEYS[10], 'terminalCount', 1)
end
return 1
""";

    public const string CancelAction = """
local actionKey = KEYS[1]
if redis.call('EXISTS', actionKey) == 0 then return { 0, '' } end
local status = tonumber(redis.call('HGET', actionKey, 'status'))
if status == 4 then return { 2, redis.call('HGET', actionKey, 'cancelled') or '' } end
local allowLocked = ARGV[2] == '1'
if status ~= 0 and not (allowLocked and status == 1) then return { 3, '' } end

local id = redis.call('HGET', actionKey, 'id')
local lane = redis.call('HGET', actionKey, 'lane')
local laneDue = redis.call('HGET', actionKey, 'laneDue')
local actionType = redis.call('HGET', actionKey, 'key')
local created = tonumber(redis.call('HGET', actionKey, 'created'))
local now = tonumber(ARGV[1])
redis.call('HSET', actionKey,
    'status', 4, 'cancelled', now, 'terminal', now,
    'locked', '', 'next', '', 'ct', ARGV[3])
redis.call('ZREM', KEYS[2], id)
redis.call('ZREM', laneDue, id)
redis.call('ZREM', KEYS[3], id)
redis.call('ZREM', KEYS[4], id)
redis.call('ZADD', KEYS[5], now, id)
redis.call('HINCRBY', KEYS[6], lane, -1)
redis.call('HINCRBY', KEYS[7], actionType, -1)
redis.call('HINCRBYFLOAT', KEYS[8], 'terminalSum', now - created)
redis.call('HINCRBY', KEYS[8], 'terminalCount', 1)
return { 1, tostring(now) }
""";

    public const string CancelReplay = """
local requestId = redis.call('HGET', KEYS[1], ARGV[1])
if not requestId then return { 0, '' } end
local replayKey = ARGV[2] .. ':replay:' .. requestId
if redis.call('EXISTS', replayKey) == 0 then return { 0, '' } end
local status = tonumber(redis.call('HGET', replayKey, 'status'))
if status == 2 then return { 2, redis.call('HGET', replayKey, 'cancelled') or '' } end
local locked = redis.call('HGET', replayKey, 'locked')
if status ~= 0 or (locked and locked ~= '') then return { 3, '' } end
redis.call('HSET', replayKey,
    'status', 2, 'cancelled', ARGV[3], 'next', '', 'ct', ARGV[4])
redis.call('ZREM', KEYS[2], requestId)
return { 1, ARGV[3] }
""";

    public const string CleanCompleted = """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local deleted = 0

local function decrementHash(key, field, amount)
    if not field or field == '' or amount == 0 then return end
    local value = redis.call('HINCRBY', key, field, -amount)
    if value <= 0 then redis.call('HDEL', key, field) end
end

local function decrementMetric(field, amount)
    if amount == 0 then return end
    local value = redis.call('HINCRBYFLOAT', KEYS[4], field, -amount)
    if tonumber(value) <= 0 then redis.call('HDEL', KEYS[4], field) end
end

for _, id in ipairs(ids) do
    local actionKey = ARGV[3] .. ':action:' .. id
    if redis.call('EXISTS', actionKey) == 1 then
        local dedupeField = redis.call('HGET', actionKey, 'dedupeField')
        local actionType = redis.call('HGET', actionKey, 'key')
        local attempts = tonumber(redis.call('HGET', actionKey, 'attempts') or '0')
        local created = tonumber(redis.call('HGET', actionKey, 'created'))
        local started = tonumber(redis.call('HGET', actionKey, 'started'))
        local completed = tonumber(redis.call('HGET', actionKey, 'completed'))
        local terminal = tonumber(redis.call('HGET', actionKey, 'terminal'))
        if dedupeField and dedupeField ~= '' then redis.call('HDEL', KEYS[2], dedupeField) end
        decrementHash(KEYS[3], actionType, attempts)
        if created and started then
            decrementMetric('enqueueExecutionSum', started - created)
            decrementMetric('enqueueExecutionCount', 1)
        end
        if started and completed then
            decrementMetric('executionSum', completed - started)
            decrementMetric('executionCount', 1)
        end
        if created and terminal then
            decrementMetric('terminalSum', terminal - created)
            decrementMetric('terminalCount', 1)
        end
        redis.call('DEL', actionKey)
    end
    redis.call('ZREM', KEYS[1], id)
    deleted = deleted + 1
end

return deleted
""";

    public const string ScheduleReplay = """
local requestKey = ARGV[1] .. ':replay:' .. ARGV[2]
if redis.call('EXISTS', requestKey) == 1 then
    local matches = redis.call('HGET', requestKey, 'source') == ARGV[4]
        and (redis.call('HGET', requestKey, 'requestedBy') or '') == ARGV[8]
        and (redis.call('HGET', requestKey, 'reason') or '') == ARGV[9]
        and (redis.call('HGET', requestKey, 'metadata') or '') == ARGV[10]
        and redis.call('HGET', requestKey, 'branch') == ARGV[7]
    if ARGV[12] == '1' then
        matches = matches and redis.call('HGET', requestKey, 'schedule') == ARGV[11]
    end
    return { matches and 1 or 4, ARGV[2] }
end

local sourceKey = ARGV[1] .. ':action:' .. ARGV[4]
if redis.call('EXISTS', sourceKey) == 0 then return { 2, '' } end
if tonumber(redis.call('HGET', sourceKey, 'status')) ~= 3 then return { 3, '' } end

local existingBranch = redis.call('HGET', KEYS[3], ARGV[13])
if existingBranch then return { 5, existingBranch } end

local parentRequest = redis.call('HGET', KEYS[2], ARGV[4])
local root = ARGV[4]
local sequence = 1
if parentRequest then
    local parentKey = ARGV[1] .. ':replay:' .. parentRequest
    if redis.call('EXISTS', parentKey) == 1 then
        root = redis.call('HGET', parentKey, 'root') or root
        sequence = tonumber(redis.call('HGET', parentKey, 'sequence') or '0') + 1
    end
end

redis.call('HSET', requestKey,
    'id', ARGV[2], 'ct', ARGV[3], 'source', ARGV[4],
    'root', root, 'action', ARGV[5], 'sequence', sequence,
    'branch', ARGV[7], 'key', redis.call('HGET', sourceKey, 'key'),
    'lane', redis.call('HGET', sourceKey, 'lane'),
    'correlation', redis.call('HGET', sourceKey, 'correlation') or '',
    'originalDedupe', redis.call('HGET', sourceKey, 'dedupe') or '',
    'status', 0, 'requestedBy', ARGV[8], 'reason', ARGV[9],
    'metadata', ARGV[10], 'requested', ARGV[6], 'schedule', ARGV[11],
    'attempts', 0, 'next', '', 'error', '', 'locked', '',
    'lockToken', '', 'materialized', '', 'cancelled', '')
redis.call('HSET', KEYS[2], ARGV[5], ARGV[2])
redis.call('HSET', KEYS[3], ARGV[13], ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[11], ARGV[2])
return { 0, ARGV[2] }
""";

    public const string ClaimReplays = """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]) * 4)
local claimed = {}
local count = 0
local now = tonumber(ARGV[1])
local timeout = tonumber(ARGV[3])

for _, id in ipairs(ids) do
    if count >= tonumber(ARGV[2]) then break end
    local replayKey = ARGV[4] .. ':replay:' .. id
    if redis.call('EXISTS', replayKey) == 0 then
        redis.call('ZREM', KEYS[1], id)
    else
        local status = tonumber(redis.call('HGET', replayKey, 'status'))
        local schedule = tonumber(redis.call('HGET', replayKey, 'schedule'))
        local nextAttempt = tonumber(redis.call('HGET', replayKey, 'next'))
        local locked = tonumber(redis.call('HGET', replayKey, 'locked'))
        local source = redis.call('HGET', replayKey, 'source')
        local due = status == 0 and schedule <= now
            and (not nextAttempt or nextAttempt <= now)
            and (not locked or locked + timeout <= now)
            and redis.call('EXISTS', ARGV[4] .. ':action:' .. source) == 1
        if due then
            local token = ARGV[5 + count * 2]
            local concurrency = ARGV[6 + count * 2]
            redis.call('HSET', replayKey,
                'locked', now, 'lockToken', token, 'ct', concurrency)
            redis.call('HINCRBY', replayKey, 'attempts', 1)
            redis.call('ZADD', KEYS[1], now + timeout, id)
            table.insert(claimed, id)
            count = count + 1
        end
    end
end
return claimed
""";

    public const string MaterializeReplay = """
if redis.call('EXISTS', KEYS[1]) == 0 then return 2 end
local status = tonumber(redis.call('HGET', KEYS[1], 'status'))
if status == 1 and redis.call('EXISTS', KEYS[2]) == 1 then return 1 end
if status ~= 0 or redis.call('HGET', KEYS[1], 'lockToken') ~= ARGV[1] then return 2 end
if redis.call('HGET', KEYS[1], 'action') ~= ARGV[2] then return 2 end

local outcome = 1
if redis.call('EXISTS', KEYS[2]) == 0 then
    local dedupeField = ARGV[12]
    if dedupeField ~= '' then redis.call('HSET', KEYS[3], dedupeField, ARGV[2]) end
    redis.call('HSET', KEYS[2],
        'id', ARGV[2], 'ct', ARGV[3], 'key', ARGV[4],
        'lane', ARGV[5], 'laneDue', ARGV[6], 'payload', ARGV[7],
        'payloadType', ARGV[8], 'metadata', ARGV[9],
        'correlation', ARGV[10], 'dedupe', ARGV[11],
        'dedupeField', dedupeField, 'status', 0, 'attempts', 0,
        'error', '', 'created', ARGV[13], 'locked', '', 'started', '',
        'next', '', 'completed', '', 'cancelled', '', 'terminal', '')
    redis.call('SADD', KEYS[5], ARGV[5])
    redis.call('ZADD', KEYS[4], ARGV[13], ARGV[2])
    redis.call('ZADD', ARGV[6], ARGV[13], ARGV[2])
    redis.call('ZADD', KEYS[6], ARGV[13], ARGV[2])
    redis.call('HINCRBY', KEYS[7], ARGV[5], 1)
    redis.call('HINCRBY', KEYS[8], ARGV[4], 1)
    outcome = 0
end

redis.call('HSET', KEYS[1],
    'status', 1, 'materialized', ARGV[13], 'locked', '',
    'lockToken', '', 'next', '', 'error', '', 'ct', ARGV[14])
redis.call('ZREM', KEYS[9], redis.call('HGET', KEYS[1], 'id'))
return outcome
""";

    public const string ReleaseReplay = """
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'status')) ~= 0 then return 0 end
if redis.call('HGET', KEYS[1], 'lockToken') ~= ARGV[1] then return 0 end
local id = redis.call('HGET', KEYS[1], 'id')
redis.call('HSET', KEYS[1],
    'locked', '', 'lockToken', '', 'error', ARGV[2],
    'next', ARGV[3], 'ct', ARGV[4])
redis.call('ZADD', KEYS[2], ARGV[3], id)
return 1
""";
}
