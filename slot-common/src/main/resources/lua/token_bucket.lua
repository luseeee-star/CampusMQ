-- token_bucket.lua
-- 网关侧令牌桶：按「学号 + IP」双维度限流（执行方案 §2.3）
-- KEYS[1] = 桶 key
-- ARGV[1] = 容量, ARGV[2] = 每秒补充速率, ARGV[3] = 当前毫秒时间戳
-- 返回 1 = 放行，0 = 拒绝
--
-- 用 Redis 而不是本地 Guava RateLimiter：应用要能从 2 台扩到 8 台，
-- 本地桶在扩容后等于把限额放大了 N 倍，限流就失效了。

local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])

if tokens == nil then
    tokens = capacity
    ts = now
end

-- 按时间差补充令牌，不超过容量
local delta = math.max(0, now - ts) / 1000.0
tokens = math.min(capacity, tokens + delta * rate)

local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now)
-- 桶过期时间设长一点，避免被误判为新桶而重置额度
redis.call('PEXPIRE', KEYS[1], 60000)
return allowed
