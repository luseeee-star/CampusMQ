-- compensate.lua —— 扣减失败后的原子回补（把「扣了但没成」还原成「没发生」）
--
-- KEYS[1] = quota:stock:{resourceId}              总余量，必须 INCR 回来
-- KEYS[2] = quota:idem:{resourceId}:{studentId}   幂等键，必须 DEL 掉，否则学生重试只会拿到"重放"
-- KEYS[3] = quota:college:{resourceId}:{college}  学院余量；未启用学院配额时传 KEYS[1] 占位
-- ARGV[1] = "1" 启用学院回补 / "0" 不启用
--
-- 【为什么回补也必须是一个脚本】
-- 客户端写 INCR 再 DEL 是两次往返，中间崩溃会留下「余量已还、幂等键还在」的组合：
-- 学生再点一次，Lua 命中幂等键，原样返回「你已抢到，余量 N」——但他上一轮其实没抢到。
-- 这条记录永远不会落库，直到对账（D9）把它揪出来，而学生手里已经有一张不存在的凭证。
-- 顺序反过来（先 DEL 再 INCR）更糟：删了幂等键但 INCR 失败，名额永久少一个。
-- 只有一个脚本能同时保证「要么都做、要么都不做」。
--
-- 【为什么 DEL 幂等键是必须的】
-- 回补意味着这次扣减作废。若保留幂等键，学生重试会被判成「重复请求」直接返回首次结果，
-- 而他手里什么也没有。删掉它，学生下一次请求才会真的重新走一遍判定。
--
-- 【已知的模糊态，故意不在这里解决】
-- 「消息发送超时但 Broker 其实收到了」时，回补会成功、消息也会被消费落库，
-- 结果 DB 比 Redis 多扣 1。这一点无法在客户端判定，只能靠对账公式
-- pending = (totalQuota - redisRemain) - (totalQuota - dbRemain) 变成负数来暴露。
-- 详见 AsyncClaimService 的注释与 LOGBOOK D3 一节。

-- 返回 {remain}：
--   {N>0}  回补后的总余量
--   {0}    库存 key 本来就不存在，什么都没做（需要 D9 对账 / D12 预热来重建）
--
-- 为什么回数组而不是单个数字：本项目所有 Lua 都按 List 接返回值。
-- StringRedisTemplate 的 valueSerializer 是字符串序列化器，声明成 RedisScript<Long>
-- 时能不能拿到 Long 取决于 Spring 的返回类型推断，而 List<String> 的路径是已经跑通的。

if redis.call('EXISTS', KEYS[1]) == 0 then
    -- 库存 key 本身丢了（Redis 重启/被删）。这时候 INCR 会凭空造出一个余量 1，
    -- 比不补更危险：对账会看到「Redis 有 1 个、DB 有 0 个」，然后怀疑是别的原因。
    -- 什么都不做，把这一条留给 D9 的重建流程（D12 的预热）。
    return {0}
end

local remain = redis.call('INCR', KEYS[1])

if ARGV[1] == '1' then
    redis.call('INCR', KEYS[3])
end
redis.call('DEL', KEYS[2])

return {remain}
