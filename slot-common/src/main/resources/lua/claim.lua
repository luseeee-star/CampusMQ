-- claim.lua —— 名额判定的唯一原子操作（需求书 §1「余量只判定一次」）
--
-- KEYS[1] = quota:stock:{resourceId}              总剩余名额
-- KEYS[2] = quota:idem:{resourceId}:{studentId}   幂等键（value = 首次扣减后的余量）
-- KEYS[3] = quota:college:{resourceId}:{college}  学院剩余配额（资源未开配额时传空串）
-- ARGV[1] = 幂等键 TTL 毫秒
-- ARGV[2] = 是否启用学院配额，"1" 启用 / "0" 不启用
--
-- 为什么必须是 Lua：SETNX + GET + DECR 若分三次往返，中间会出现
-- 「幂等标记已写但库存没扣」或「库存扣了但幂等没写」的中间态，重试即双扣。
-- Redis 单线程内原子执行，把这几步合成一步。
--
-- 返回 {code, remain}，两个都是数字（Redis 的 nil 会被截断，所以不留空位）：
--   { 0, N}  申领成功，N = 扣减后的总余量
--   {-1, 0}  总名额已满（R-1 不允许超发）
--   {-2, N}  重复请求（R-3），N = 首次扣减后的余量 → 用它能原样复现首次结果
--   {-3,-1}  库存 key 不存在，P0 信号：缓存丢了，按系统繁忙处理，绝不能当「已满」放过去
--   {-4, 0}  该学院配额已满（G-1），但总名额还有 → 语义必须和「全场已满」分开
--
-- 顺序不能颠倒：先判幂等再判余量。
-- 反过来的话，一个已经抢到的人在名额刚满时重试，会收到「名额已满」，
-- 而他其实已经占了一个名额——这是最典型的「学生以为没抢到，反复刷新」的来源（P1）。

local stockKey = KEYS[1]
local idemKey = KEYS[2]
local collegeKey = KEYS[3]
local idemTtl = tonumber(ARGV[1])
local useCollege = ARGV[2] == '1'

if redis.call('EXISTS', idemKey) == 1 then
    local first = tonumber(redis.call('GET', idemKey))
    if first == nil then
        first = 0
    end
    return {-2, first}
end

local stock = tonumber(redis.call('GET', stockKey))
if stock == nil then
    return {-3, -1}
end

if stock <= 0 then
    return {-1, 0}
end

if useCollege then
    local collegeStock = tonumber(redis.call('GET', collegeKey))
    if collegeStock == nil then
        -- 学院配额 key 缺失同样属于「配置没灌进去」，不能默认放行
        return {-3, -1}
    end
    if collegeStock <= 0 then
        return {-4, 0}
    end
    redis.call('DECR', collegeKey)
end

local remain = redis.call('DECR', stockKey)

-- value 存扣减后的余量：重复请求靠它复现首次结果（R-3）
redis.call('SET', idemKey, remain, 'PX', idemTtl)

return {0, remain}
