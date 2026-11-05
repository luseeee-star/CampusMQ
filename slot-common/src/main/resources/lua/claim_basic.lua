-- claim_basic.lua —— D2 对照组②：只证明「原子扣减能消灭超卖」
--
-- 这是 claim.lua 的**降级版**，故意不包含 D4 才需要的能力：
--   · 不返回首次扣减后的余量（R-3 重放语义）
--   · 不判定学院配额（G-1）
--   · 不校验资格名单（P4）
-- 保留这些能力会让「Redis 这一层挡住了多少」和 D4 的功能混在一起，
-- 对照组之间的差异就不再是单一变量了。
--
-- KEYS[1] = quota:stock:{resourceId}              总剩余名额
-- KEYS[2] = quota:idem:{resourceId}:{studentId}   幂等键（value = "1"）
-- ARGV[1] = 幂等键 TTL 毫秒
--
-- 返回 {code}：
--    1  抢到（已扣减）
--    0  名额已满
--   -1  该学生已参与过
--   -2  库存 key 不存在 —— 这不是业务返回码，是 P0 系统信号：
--       缓存没灌进去或被人删了。绝不能当成「已满」，否则学生会以为名额没了，
--       而实际上是我们自己把数据弄丢了。
--
-- 为什么必须是 Lua：GET → 判断 → DECR → SET 若分四次往返，中间会出现
-- 「幂等标记已写但库存没扣」或「库存扣了但幂等没写」的中间态，重试即双扣。
-- Redis 单线程执行脚本，执行期间不会被打断，这几步天然原子。
--
-- 顺序不能颠倒：先判幂等，再判余量。
-- 反过来的话，已经抢到的人在名额刚满时重试会收到「名额已满」，
-- 而他其实已经占了一个名额 —— 这正是「学生以为没抢到、反复刷新」的来源（P1）。

local stockKey = KEYS[1]
local idemKey = KEYS[2]
local idemTtl = tonumber(ARGV[1])

if redis.call('EXISTS', idemKey) == 1 then
    return {-1}
end

local stock = tonumber(redis.call('GET', stockKey))
if stock == nil then
    return {-2}
end

if stock <= 0 then
    return {0}
end

redis.call('DECR', stockKey)
redis.call('SET', idemKey, '1', 'PX', idemTtl)

return {1}
