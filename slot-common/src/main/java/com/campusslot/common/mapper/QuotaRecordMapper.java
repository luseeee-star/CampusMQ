package com.campusslot.common.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.campusslot.common.entity.QuotaRecord;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 状态迁移全部用「条件更新 + 受影响行数」代替「先查再改」，把幂等与并发控制合并成一次数据库操作。
 * 受影响行数为 0 就说明这条消息是重复投递或状态已变，直接 ACK，不做任何副作用。
 *
 * <p>这组方法同时是「同一学生重复取消 10 次、余量只回补 1 次」（§8.1）的实现基础：
 * 第二次取消时 {@code status = 'LOCKED'} 不成立，返回 0，调用方不得再动 Redis 余量。
 */
@Mapper
public interface QuotaRecordMapper extends BaseMapper<QuotaRecord> {

    /**
     * 第三层防超卖：唯一索引兜底（R-2），重复投递/重试/狂点都落不进来。
     *
     * <p><b>这里曾经是 {@code INSERT IGNORE}，2026-10-27 改掉了。</b>
     * {@code IGNORE} 会把一批<b>不该被忽略</b>的错误一起降级成「受影响行数 0」：
     * <pre>
     *   CHECK 约束失败        → 跳过该行，affected = 0   → 被误判成「重复投递」，ACK 掉，消息永久消失
     *   外键失败              → 跳过该行，affected = 0   → 同上
     *   NOT NULL 缺字段        → 插入隐式默认值，affected = 1 → 更糟：脏数据落库，还回报成功
     * </pre>
     * 现在用普通 INSERT，让错误如实抛出，由调用方按异常类型分流：
     * 唯一键冲突 → {@code DuplicateKeyException} → 真正的重复；
     * 其余 → {@code DataAccessException} → 交给 MQ 重试。
     *
     * <p><b>为什么不用 {@code ON DUPLICATE KEY UPDATE}</b>：MySQL Connector/J 默认
     * {@code useAffectedRows=false}（返回 found rows），冲突时它返回的是 <b>1 而不是 0</b>，
     * 会把重复直接判成插入成功 —— 同一个坑换了个位置，而且更难看出来。
     *
     * <p><b>{@code created_at / updated_at} 由调用方传入，不用 {@code NOW()}</b>：
     * 这两个值今后要与 {@code released_at} 一起算「留存多久、多久被处理」，
     * 而服务器时区与应用时区只要差 8 小时，同一行里的时间列就互相矛盾。
     * 调用方统一用 {@code AppTime.now()} 赋值（见 {@link QuotaRecord#getCreatedAt()} 的写入点）。
     *
     * @return 恒为 1（插入成功）；冲突时抛异常，不再返回 0
     */
    @Insert("""
            INSERT INTO quota_record
                (resource_id, dept_id, student_id, status, source, claim_at, expire_at, created_at, updated_at)
            VALUES
                (#{resourceId}, #{deptId}, #{studentId}, #{status}, #{source}, #{claimAt}, #{expireAt},
                 #{createdAt}, #{updatedAt})
            """)
    int insertRecord(QuotaRecord record);

    /**
     * 确认：返回 1 才算确认成功；返回 0 说明已确认、已释放，或<b>已过确认时限</b>。
     *
     * <p><b>为什么要带 {@code expire_at > now} 而不是只判 status</b>：超时释放（SYS-1）
     * 是 500ms 一次的扫描，从"记录过期"到"被扫到并释放"之间存在一个最长约一个周期的窗口。
     * 只判 status 的话，学生可以在这个窗口里把一个已经超时的名额确认掉，
     * 于是"30 分钟确认时限"在最需要它的边界上失效——而且这个窗口在压测里几乎撞不到。
     *
     * <p><b>为什么 {@code now} 是参数而不是 {@code NOW()}</b>：{@code NOW()} 用的是
     * MySQL 服务器时区，而 {@code expire_at} 是应用侧按 {@link com.campusslot.common.constant.AppTime#ZONE}
     * 算出来写进去的 {@code LocalDateTime}。两者一旦不一致（CI 容器默认 UTC），
     * 这条条件会整体偏移 8 小时：要么所有确认都判超时，要么全部放行。传参只有一处时钟。
     */
    @Update("""
            UPDATE quota_record
               SET status = 'CONFIRMED', confirmed_at = #{now}, version = version + 1, updated_at = #{now}
             WHERE id = #{recordId} AND status = 'LOCKED' AND expire_at > #{now}
            """)
    int confirmIfLocked(@Param("recordId") long recordId, @Param("now") LocalDateTime now);

    /**
     * 按 (resourceId, studentId) 确认，<b>一次原子 UPDATE 完成"定位 + 校验持有 + 状态迁移"</b>。
     *
     * <p>为什么不写成「先 {@code selectByStudent} 拿 id，再 {@code confirmIfLocked}」：
     * 那是典型的"先查再改"，两条语句之间记录可能被超时释放扫走，于是查到的 LOCKED 与
     * 更新的前提不再一致，返回 1 行却是在错误的前提上成功的。写成复合条件后，
     * "这条记录此刻是否属于这个学生、是否还是 LOCKED、是否还没超时"由同一条 UPDATE 裁决，
     * 受影响行数就是唯一答案，也不需要 {@code recordId} 出现在请求里。
     *
     * <p>返回 0 的四种可能必须分开处理（调用方查一次 {@code selectByStudent} 区分）：
     * <ul>
     *   <li>{@code CONFIRMED} → 幂等成功，返回 200（重复点击不是错误）</li>
     *   <li>{@code RELEASED} → 409，目标状态与当前状态互斥</li>
     *   <li>{@code LOCKED} 且已过期 → 409，提示超过确认时限</li>
     *   <li>无记录 → 404</li>
     * </ul>
     */
    @Update("""
            UPDATE quota_record
               SET status = 'CONFIRMED', confirmed_at = #{now}, version = version + 1, updated_at = #{now}
             WHERE resource_id = #{resourceId} AND student_id = #{studentId}
               AND status = 'LOCKED' AND expire_at > #{now}
            """)
    int confirmByStudent(@Param("resourceId") long resourceId,
                         @Param("studentId") String studentId,
                         @Param("now") LocalDateTime now);

    /**
     * 释放：返回 1 才执行 Redis INCR 与补位通知；返回 0 说明已确认或已释放，直接 ACK。
     * 条件是 {@code status = 'LOCKED'} 而不是「released_at 为空」——后者在并发下会双回补。
     *
     * <p><b>D8 的取消路径必须同时做三件事，顺序不能换</b>（D8 实现时照这里写）：
     * <ol>
     *   <li>本方法（DB 状态迁移，它是唯一裁决者）；</li>
     *   <li>{@code QuotaResourceMapper.increaseRemain}（DB 权威余量 +1，同一事务内）；</li>
     *   <li>Redis {@code release.lua}（INCR 镜像余量）。</li>
     * </ol>
     * 只做 ①③ 不做 ② 的话，Redis 是 N+1、DB 还是 N，两边永久差 1，D9 对账会一直红——
     * 而这类差异不会自己收敛，因为没有任何路径会让 DB 的 remain 再涨回来。
     *
     * <p><b>Redis 侧的幂等键必须保留，取消时绝不能 DEL。</b>理由不是"防超发"，而是
     * <b>防库存泄漏</b>：删了键，学生再点一次会走完整的 Lua 扣减（stock -1）并投消息，
     * 消费端 {@code INSERT} 撞 {@code uk_record_resource_student} → DUPLICATE → ACK 且
     * <b>故意不回补</b>（那是防重投的正确做法）。结果是 Redis 少一个、DB 没有对应记录，
     * 也就是"少发"。唯一索引挡住了同一个人拿两次，所以不超发；但少发同样要 D9 去修。
     * 保留幂等键则重复请求直接返回首次结果（R-3 语义本来就要求这样），零副作用。
     *
     * <p><b>{@code released_at} 也必须传参</b>，虽然它不参与 WHERE 判定：
     * NFR-5 的「释放误差 = {@code released_at} − {@code expire_at}」是拿两列相减，
     * 而 {@code expire_at} 是应用侧按 Asia/Shanghai 写的。若 {@code released_at} 用
     * 服务器 UTC 的 {@code NOW()}，这个差恒为 −28,800 秒，判据会变成"永远合格"，
     * 看起来还像是超额达标。
     */
    @Update("""
            UPDATE quota_record
               SET status = 'RELEASED', released_at = #{now}, release_reason = #{reason},
                   version = version + 1, updated_at = #{now}
             WHERE id = #{recordId} AND status = 'LOCKED'
            """)
    int releaseIfLocked(@Param("recordId") long recordId,
                        @Param("reason") String reason,
                        @Param("now") LocalDateTime now);

    /**
     * 取消：按 {@code (resourceId, studentId)} 释放，与 {@link #confirmByStudent} 对称。
     *
     * <p><b>为什么不先 {@code selectByStudent} 拿 id 再调 {@link #releaseIfLocked}</b>：
     * 同样的「先查再改」竞态——查到 LOCKED 与更新之间，SYS-1 可能已经把这条扫走释放了，
     * 于是这次取消更新到 0 行，却会因为「查到的是 LOCKED」被误判成内部错误。
     * 写成复合条件后，受影响行数就是唯一答案。
     *
     * <p><b>为什么条件里没有 {@code expire_at > now}</b>（与确认侧刻意不同）：
     * 确认必须拒绝过期记录（否则「30 分钟时限」在最需要它的边界上失效）；
     * 而取消一条已过期但仍 LOCKED 的记录，结果与超时释放完全一致（RELEASED + 回补），
     * 拒绝它只会让学生看到「当前状态不允许该操作」却什么也没发生，
     * 随后 SYS-1 又把它释放了——用户视角是「点了没反应，过一会自己好了」。
     *
     * <p>返回 0 的四种可能由调用方回查区分：
     * {@code RELEASED} → 幂等成功 200 / {@code CONFIRMED} → 409（目标状态互斥）/ 无记录 → 404 /
     * {@code LOCKED}（并发窗口内又变回 LOCKED 不可能，但若是则按 409 处理，不掩盖异常）。
     */
    @Update("""
            UPDATE quota_record
               SET status = 'RELEASED', released_at = #{now}, release_reason = #{reason},
                   version = version + 1, updated_at = #{now}
             WHERE resource_id = #{resourceId} AND student_id = #{studentId} AND status = 'LOCKED'
            """)
    int releaseByStudent(@Param("resourceId") long resourceId,
                         @Param("studentId") String studentId,
                         @Param("reason") String reason,
                         @Param("now") LocalDateTime now);

    /** 已发放数 = 锁定中 + 已确认（已释放的不再占名额）。 */
    @Select("""
            SELECT COUNT(*) FROM quota_record
             WHERE resource_id = #{resourceId} AND status IN ('LOCKED', 'CONFIRMED')
            """)
    long countIssued(@Param("resourceId") long resourceId);

    /**
     * SYS-1 兜底扫表：定时消息丢了的记录靠这里捞出来，走 (status, expire_at) 联合索引。
     *
     * <p><b>边界与 {@code confirmByStudent} 必须完全对称</b>：确认侧要求
     * {@code expire_at > now} 才允许确认，释放侧就必须在 {@code expire_at <= now} 时释放。
     * 若一边用 {@code <} 一边用 {@code <=}，{@code expire_at == now} 那一秒会既不能确认
     * 也不被释放——记录卡在 LOCKED，直到下一次扫描仍是同一秒才动。
     *
     * <p>{@code now} 同样是参数：见 {@link #confirmIfLocked} 里"为什么不用 NOW()"。
     */
    @Select("""
            SELECT * FROM quota_record
             WHERE status = 'LOCKED' AND expire_at <= #{now}
             ORDER BY expire_at
             LIMIT #{limit}
            """)
    List<QuotaRecord> selectExpiredLocked(@Param("now") LocalDateTime now, @Param("limit") int limit);

    /** R-3 兜底：Redis 幂等键过期后，靠这张表把首次结果找回来（唯一索引保证最多一条）。 */
    @Select("""
            SELECT * FROM quota_record
             WHERE resource_id = #{resourceId} AND student_id = #{studentId}
            """)
    QuotaRecord selectByStudent(@Param("resourceId") long resourceId,
                                @Param("studentId") String studentId);
}
