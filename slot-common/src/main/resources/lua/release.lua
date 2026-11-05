-- release.lua —— 取消 / 超时释放时回补 Redis 镜像余量
--
-- KEYS[1] = quota:stock:{resourceId}                     总余量，要 INCR
-- KEYS[2] = quota:college:{resourceId}:{collegeCode}     学院余量；未启用学院配额时传 KEYS[1] 占位
-- ARGV[1] = totalQuota（DB 事务内读出的权威上限）
-- ARGV[2] = "1" 回补学院 / "0" 不回补
--
-- 返回 {code, remain}：
--   {0,  N}   成功，N = 回补后的总余量
--   {-1, 0}   总余量 key 不存在/值不是数字，什么都没做（不能 INCR，否则凭空造出余量 1）
--   {-2, N}   已到 totalQuota 上界，什么都没做（N = 当前余量，供对账记录）
--   {-3, N}   要回补学院，但学院 key 不存在/值不是数字，什么都没做
--   {-4, N}   KEYS[2] 与 KEYS[1] 是同一个 key（传参错误），什么都没做
--
-- 所有判定都写在第一个 INCR 之前：Redis 脚本执行到一半报错不会回滚已执行的写命令，
-- 一旦"总余量加了、学院没加"，这笔就变成半个事务，只能靠对账去猜。宁可整笔不做。
--
-- 【为什么和 compensate.lua 是两个脚本，而不是复用一个】
-- 两者方向相反。compensate 是「扣了但没成」，必须 DEL 幂等键让学生能重试；
-- release 是「真的放弃/过期了」，幂等键必须保留 —— 删了它，学生再点一次会走完整扣减
-- （stock -1）并投消息，消费端 INSERT 撞 uk_record_resource_student → DUPLICATE → ACK
-- 且故意不回补。结果是 Redis 少一个、DB 没有对应记录，也就是「少发」。
-- 唯一索引挡住了同一个人拿两次（不超发），但少发同样要 D9 去修。
-- 保留幂等键则重复请求直接返回首次结果（R-3 语义本来就要求这样），零副作用。
--
-- 【为什么 -1 不能 INCR】
-- INCR 对不存在的 key 是「创建并置 1」。库存 key 丢失（Redis 重启、被删、未预热）时
-- 这等于凭空发一个名额，且对账会看到「Redis 有 1、DB 有 0」，然后去查错方向。
-- compensate.lua 已经踩过这个坑（它同样返回 0），这里保持同一处置。
--
-- 【为什么上界是 cur >= total 而不是 cur > total】
-- remain 的合法区间是 [0, total]。cur == total 说明已经满了，再 +1 就是 101/100。
-- 注意这个上界只是**最后一道保险**：真正应该拦住超量回补的是 DB 的
-- increaseRemain（WHERE remain < total_quota），它返回 0 时调用方已经会落对账。
-- Redis 侧再判一次，是为了防止「DB 到顶、Redis 还没到顶」这种已经漂移的状态被这一笔
-- 越推越远。
--
-- 【为什么 totalQuota 要从 ARGV 传，不在这里读】
-- 只有 Redis 侧的库存 key，拿不到配额上限。而 Redis 里没有 total 的副本可读：
-- 从 meta 缓存（quota:res:{id}）拿也不行 —— 它的 TTL 是 5 秒，活动扩容后 5 秒内
-- 会把合法回补误判成 -2，落一条假对账。调用方必须在 DB 事务内读 total_quota，
-- 保证「DB 已 +1」与「上界判定」基于同一个提交点的数据。

-- 【为什么学院侧也要 EXISTS 检查】
-- 总余量那侧的 EXISTS 防的是"凭空造出余量 1"；学院侧 INCR 对这个 key 是同一个动作：
-- key 不存在 → 创建并置 1。而学院 key 的丢失是真实存在的场景——Redis 重启后只重建了
-- stock、或某个 key 被误删。此时 `INCR quota:college:3:A01` 会凭空造出一个孤立 key：
-- 它的值从 1 开始（不是 quota），此后每次扣减都从 1 往下走，而 DB 侧 remain 是满的，
-- 两边永远对不上，且没有任何路径会让它回到正确值。
--
-- Java 侧 resolveCollege() 只挡住了"活动没配学院配额行"这一半（countByResource == 0）；
-- "配了行但 Redis key 丢了"那一半只能在这里挡。两道防线的分工必须写清楚，
-- 否则下一个人会以为 countByResource 已经够了。
--
-- 为什么不像总余量那样再判一次学院上界：那需要多传一个 collegeQuota 参数，而 DB 侧
-- increaseRemain（WHERE remain < quota）已经判过并返回 0 → 调用方落 COLLEGE_AT_TOTAL。
-- 在这里再判一次，收益是"少一次已被 DB 拦住的漂移"，代价是"ARGV 缺省时如何区分
-- '不判上界' 与 '上界真的是 0'"这个新的歧义。EXISTS 不同：它防的是凭空造 key，
-- 没有第二道防线，所以必须有。

-- 【为什么 KEYS[2] 允许用 KEYS[1] 占位，但占位时要断言】
-- 未启用学院配额时 Java 侧传 KEYS[2] = KEYS[1]（避免构造一个不存在的 key 名），
-- ARGV[2] = '0' 保证不会碰它。这是第三处同类模式（claim.lua 的 KEYS[3] 同款），
-- 风险绑在"ARGV[2] 与 collegeCode 必须同源传参"上。现在 EXISTS 检查让它变得致命：
-- 若哪天有人误传 ARGV[2]='1' 而 KEYS[2] 仍是占位，EXISTS 会通过，然后同一个 key 被
-- INCR 两次 —— 总余量凭空 +1，正是这套防线要防的事。所以这里加一条断言，
-- 把这个模式从"靠注释"升级成"靠代码"。

if redis.call('EXISTS', KEYS[1]) == 0 then
    return {-1, 0}
end

local raw = redis.call('GET', KEYS[1])
if raw == false then
    return {-1, 0}
end

local cur = tonumber(raw)
if cur == nil then
    -- 值不是数字：说明这个 key 被别的写入污染了，不能猜，交给对账
    return {-1, 0}
end

local total = tonumber(ARGV[1])
if total == nil then
    -- 参数缺失属于编程错误，按「不知道该不该加」处理，绝不冒险 INCR
    return {-2, cur}
end

if cur >= total then
    return {-2, cur}
end

local needCollege = ARGV[2] == '1'
if needCollege then
    if KEYS[2] == KEYS[1] then
        return {-4, cur}
    end
    if redis.call('EXISTS', KEYS[2]) == 0 then
        return {-3, cur}
    end
    local collegeRaw = redis.call('GET', KEYS[2])
    if collegeRaw == false or tonumber(collegeRaw) == nil then
        return {-3, cur}
    end
end

local remain = redis.call('INCR', KEYS[1])

if needCollege then
    redis.call('INCR', KEYS[2])
end

return {0, remain}
