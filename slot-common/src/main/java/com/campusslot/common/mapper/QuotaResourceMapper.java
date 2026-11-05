package com.campusslot.common.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.campusslot.common.entity.QuotaResource;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * 第二层防超卖：条件更新 + 行锁，落库侧绝对不超发（需求书 §8.2）。
 * 用 `remain > 0` 做条件而不是先查再改，把并发控制合并进一次 UPDATE。
 *
 * <p>{@code updated_at} 用参数而不是 {@code NOW()}：它不参与判定，但 D9 对账会按
 * 「最后一次变动时间」划定比对窗口，两套时钟混用会让窗口算错 8 小时。
 */
@Mapper
public interface QuotaResourceMapper extends BaseMapper<QuotaResource> {

    @Update("""
            UPDATE quota_resource
               SET remain = remain - 1, version = version + 1, updated_at = #{now}
             WHERE id = #{resourceId} AND remain > 0
            """)
    int deductRemain(@Param("resourceId") long resourceId, @Param("now") LocalDateTime now);

    /**
     * 释放回补：只有确实扣过减的才回补，防止把 remain 加超总量。
     *
     * <p><b>这个方法至今没有调用点，D8 必须接上。</b>{@code remain} 是 DB 侧的权威余量
     * （{@code deductRemain} 在消费端每落库一条就调一次），而 D8 的取消 / 超时释放
     * 只改 {@code quota_record.status} 不会让它变。少了这一步就是"Redis 涨了、DB 没涨"，
     * 两边永久差 1：`pending` 正数不收敛，D9 对账每次巡检都会报同一条差异。
     *
     * <p>{@code WHERE remain < total_quota} 是 DB 侧的防超上界，与 {@code release.lua}
     * 里 {@code remain > total} 的 Redis 侧上界对称。两处都要有：前者防并发回补把
     * 余量加超总量，后者防"取消风暴"时 Redis 单独漂移。返回 0 说明已经顶到上限，
     * 这不是可以忽略的情况，必须告警。
     *
     * <p>与 {@code releaseIfLocked} 必须在<b>同一个事务</b>里：先改状态再加余量，
     * 中间失败会留下"记录已释放但名额没还"的账，比对账更难修。
     *
     * <p><b>返回 0 时调用方不得 INCR Redis</b>（S-4 的实现约束）：0 表示 DB 已经顶到
     * {@code total_quota}，说明两边本来就漂了。此时若照常给 Redis +1，差异从一个方向
     * 翻到另一个方向且不会自愈——正确动作是跳过 Redis 并落 {@code reconcile_diff}。
     */
    @Update("""
            UPDATE quota_resource
               SET remain = remain + 1, version = version + 1, updated_at = #{now}
             WHERE id = #{resourceId} AND remain < total_quota
            """)
    int increaseRemain(@Param("resourceId") long resourceId, @Param("now") LocalDateTime now);

    /**
     * 事务内读余量与上限，<b>当前读 + 行锁</b>（{@code FOR UPDATE}）。
     *
     * <p>为什么不能普通 {@code SELECT}：InnoDB 的可重复读下，事务内第一次一致性读就固定了快照。
     * 若并发有人在扩容（改 {@code total_quota}），快照读会拿到旧上限，
     * 于是「DB 已 +1、Redis 还没到新上界」这种合法回补被判成 -2，落一条假对账。
     * 当前读保证读到的是最新已提交值，并且与 {@code increaseRemain} 的 UPDATE 串行化在同一行上。
     *
     * <p>必须在<b>事务内</b>调用：事务外的 {@code FOR UPDATE} 是自动提交的，锁立刻释放，
     * 拿到的值与后面的操作不在同一个提交点上。
     */
    @Select("""
            SELECT remain, total_quota FROM quota_resource WHERE id = #{resourceId} FOR UPDATE
            """)
    Map<String, Object> selectRemainAndTotalForUpdate(@Param("resourceId") long resourceId);
}
