local key = KEYS[1]
local operation = ARGV[1]
local token = ARGV[2]
local kind = redis.call('TYPE', key).ok
if kind ~= 'zset' and kind ~= 'none' then
    return redis.error_reply('unexpected recording reservation key type')
end

local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', key, '-inf', string.format('%.0f', now))

if operation == 'complete' then
    return redis.call('ZREM', key, token)
elseif operation == 'owns' then
    if redis.call('ZSCORE', key, token) then return 1 end
    return 0
end

local expires = now + tonumber(ARGV[3])
-- Redis scores and Lua numbers both use doubles.
if expires > 9007199254740991 then
    return redis.error_reply('recording reservation expiration exceeds integer precision')
end
local score = string.format('%.0f', expires)
if operation == 'reserve' then
    if redis.call('ZCARD', key) >= tonumber(ARGV[4]) then return 0 end
    if redis.call('ZADD', key, 'NX', score, token) ~= 1 then return 0 end
elseif operation == 'renew' then
    if not redis.call('ZSCORE', key, token) then return 0 end
    redis.call('ZADD', key, 'XX', score, token)
else
    return redis.error_reply('unknown recording reservation operation')
end

-- Expire idle scopes. One short lease must never shorten another token's lease.
local latest = redis.call('ZRANGE', key, -1, -1, 'WITHSCORES')
redis.call('PEXPIREAT', key, string.format('%.0f', tonumber(latest[2])))
return 1
