package com.campusslot.common.constant;

/**
 * Redis key 约定。
 *
 * <p>注意：库存 key 一旦丢失，Lua 读到 nil 会返回 -3（按系统繁忙处理），绝不能当作「已满」放过去。
 * 因此 Redis 必须显式 {@code maxmemory-policy noeviction}，且把「库存 key 是否存在」列为 P0 告警。
 *
 * <p>v2.0 去掉了抽签种子位（{@code quota:seed:*}）：§9 明确不做抽签，没有开奖也就没有种子公示。
 */
public final class RedisKeys {

    private RedisKeys() {
    }

    /** 剩余名额，String。source of truth 为 DB，此值为预扣用镜像。 */
    public static String stock(long resourceId) {
        return "quota:stock:" + resourceId;
    }

    /**
     * 幂等标记。value 存的是<b>首次申领扣减后的余量</b>，不是 "1"。
     *
     * <p>R-3 要求重复请求返回首次结果，所以这里必须能还原出首次的结果；
     * 只存 "1" 的实现，重复点击时除了「你已参与过」什么都答不出来。
     * TTL = 确认时限 + 冗余 10min。
     */
    public static String idem(long resourceId, String studentId) {
        return "quota:idem:" + resourceId + ":" + studentId;
    }

    /** 学院剩余配额（G-1「按学院设配额」）。资源未开配额时该 key 不存在。 */
    public static String collegeStock(long resourceId, String collegeCode) {
        return "quota:college:" + resourceId + ":" + collegeCode;
    }

    /**
     * 整份名单的 key。<b>当前没有任何调用点，实际生效的是 {@link #rosterEntry}。</b>
     *
     * <p>保留它只是为了记录早期设计（把整份名单做成一个 Set 或 Hash）。
     * 那个方案在 Redis Cluster 下会变成热点 key，D4 起已改成单人视图，
     * 所以灌名单请用 {@link #rosterEntry}，不要用这个 —— 用了会写出一个没人读的 key，
     * 症状是"名单里的人全被判成不在名单内"。
     *
     * @deprecated 用 {@link #rosterEntry(long, String)} 逐人写，不要写整份名单
     */
    @Deprecated
    public static String roster(long resourceId) {
        return "quota:roster:" + resourceId;
    }

    /**
     * 资格名单的<b>单人视图</b>：value = 该学生的学院编码。D4 起用它。
     *
     * <p>为什么不是把整份名单做成 Hash 再 {@code HGET}：Hash 在 Redis Cluster 里是单个 slot，
     * 一个 3 万人的名单会变成热点 key，多台 API 机器全打在同一个分片上。
     * 单 key 单人是 O(1) 且天然分散，代价是灌名单时多写 N 个 key——那是离线动作。
     *
     * <p>这也是「学院配额」能进同一个 Lua 的前提：学院编码必须在 EVAL 之前就能拿到，
     * 否则要么多一次往返（Java 侧 GET），要么在脚本里访问未声明的 key。
     * 当前实现是前者：仅当活动开了学院配额时才 GET 这一次，默认路径零开销。
     */
    public static String rosterEntry(long resourceId, String studentId) {
        return "quota:roster:" + resourceId + ":" + studentId;
    }

    /** {@link #rosterEntry} 的前缀，用于 SCAN 清理（不要用 KEYS）。 */
    public static String rosterEntryPrefix(long resourceId) {
        return "quota:roster:" + resourceId + ":";
    }

    /** 资源元信息缓存（开放时刻、确认时限、取消截止）。 */
    public static String resourceMeta(long resourceId) {
        return "quota:res:" + resourceId;
    }

    /** 单学生单资源令牌桶（P4「按学号限流」）。 */
    public static String rateStudent(String studentId, long resourceId) {
        return "rl:s:" + studentId + ":" + resourceId;
    }

    /** 全局令牌桶。 */
    public static String rateGlobal() {
        return "rl:global";
    }
}
