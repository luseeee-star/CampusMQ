package com.campusslot.app.claim;

import com.campusslot.common.api.ErrorCode;
import com.campusslot.common.constant.AppTime;
import com.campusslot.common.constant.StudentIdRule;
import com.campusslot.common.entity.QuotaRecord;
import com.campusslot.common.mapper.QuotaRecordMapper;
import com.campusslot.common.notify.NotifyEvent;
import com.campusslot.common.notify.NotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * 确认（S-3）：{@code LOCKED → CONFIRMED}，名额最终归属。
 *
 * <p><b>确认是"可靠路径"上的第一个动作</b>，与申领的性质完全不同：
 * <ul>
 *   <li>申领是一次瞬时冲击（11k/s），所以走 Redis + MQ 异步，允许最终一致；</li>
 *   <li>确认是单个人一次点击，量级差三个数量级，所以<b>同步写 DB</b>——
 *       学生点完必须立刻知道结果，走队列再回查反而制造"点了没反应"。</li>
 * </ul>
 * 因此这个类里没有 Lua、没有 MQ，只有一次条件 UPDATE 和一次通知入账。
 *
 * <p><b>为什么现在有 {@code @Transactional}</b>（2026-11-04，SYS-2 落地时改的）：
 * 在这之前整个动作只有一条 UPDATE，它自己就是原子的，加事务只是多一次 begin/commit。
 * 但通知入账是<b>第二条写</b>，"单条即原子"的前提就不成立了：
 * <ul>
 *   <li>写在事务外（提交之后）：提交成功、写通知前进程崩了，就会出现
 *       "名额已 CONFIRMED 但通知记录不存在"——通知是<b>事件</b>不是状态，
 *       D9 对账能对齐余量，对齐不了"该发没发"，这条缺口没有任何地方能补；</li>
 *   <li>写在事务内并一起回滚：最坏结果是学生看到一次失败，而确认接口本来就有 replay 分支
 *       （再点一次就是一次完整的重试），代价是"多点一次"，而不是"永久少一条通知"。</li>
 * </ul>
 * 两种失败模式里选可逆的那个——与释放路径"先 DB 后 Redis"是同一条纪律。
 */
@Service
public class ConfirmService {

    private static final Logger log = LoggerFactory.getLogger(ConfirmService.class);

    private static final String STATUS_CONFIRMED = "CONFIRMED";
    private static final String STATUS_RELEASED = "RELEASED";
    private static final String STATUS_LOCKED = "LOCKED";

    private final QuotaRecordMapper recordMapper;
    private final NotificationService notificationService;

    public ConfirmService(QuotaRecordMapper recordMapper,
                          NotificationService notificationService) {
        this.recordMapper = recordMapper;
        this.notificationService = notificationService;
    }

    /**
     * 确认一条 {@code (resourceId, studentId)} 的记录。
     *
     * <p>判据顺序：入口先挡非法学号（与申领同一个 {@link StudentIdRule}，两处口径必须一致，
     * 否则会出现"能抢到但确认不了"的记录）→ 一次条件 UPDATE + 一次通知入账（同一事务）
     * → 只在返回 0 时才回查状态。成功路径上两次写、一次读。
     */
    @Transactional
    public ConfirmOutcome confirm(long resourceId, String studentId) {
        if (!StudentIdRule.isValid(studentId)) {
            return ConfirmOutcome.fail(ErrorCode.PARAM_INVALID, StudentIdRule.rejectReason(studentId));
        }

        // 与 expire_at 用同一个时钟：expire_at 是应用侧按 AppTime.ZONE 写入的 LocalDateTime，
        // 用 DB 的 NOW() 比较会在容器 TZ≠Asia/Shanghai 时整体偏移（见 Mapper 注释）。
        LocalDateTime now = AppTime.fromEpochMs(AppTime.nowMs());

        int rows = recordMapper.confirmByStudent(resourceId, studentId, now);
        if (rows == 1) {
            // 同一个事务：通知写失败会让上面的 UPDATE 一起回滚，学生重试即可（replay 分支安全）。
            // bizId 用 (resourceId, studentId) 而不是 recordId——确认路径刻意不回查主键，
            // 而 uk_record_resource_student 保证两者等价。重复点击时 INSERT IGNORE 命中唯一键，
            // 不会产生第二条通知，也不会把已经 DEAD 的通知重置回 PENDING。
            notificationService.enqueue(NotifyEvent.CONFIRM, resourceId, studentId);
            log.debug("确认成功 resourceId={} studentId={}", resourceId, studentId);
            return ConfirmOutcome.ok();
        }

        // rows == 0 的四种可能，必须分开答：它们的 HTTP 语义完全不同（200 / 409 / 404）。
        QuotaRecord record = recordMapper.selectByStudent(resourceId, studentId);
        if (record == null) {
            // 从没抢到过，或者抢到了但记录还没落库（异步链路的 pending 窗口）。
            // 两种都回 404：此刻"查无此记录"是事实，前端据此提示"先申领"。
            return ConfirmOutcome.fail(ErrorCode.RECORD_NOT_FOUND,
                    "没有找到你的申领记录；若刚抢到，请稍后重试（消息可能还在落库）");
        }

        String status = record.getStatus();
        if (STATUS_CONFIRMED.equals(status)) {
            // 幂等：重复点击返回当前状态 200，不报错
            return ConfirmOutcome.replay();
        }
        if (STATUS_RELEASED.equals(status)) {
            // 目标状态与当前状态互斥：已释放的名额不能再确认（名额已经回补给别人了）
            return ConfirmOutcome.fail(ErrorCode.RECORD_STATUS_ILLEGAL,
                    "名额已释放（" + nullToDash(record.getReleaseReason()) + "），无法确认");
        }
        if (STATUS_LOCKED.equals(status)) {
            // 走到这里的前提是「UPDATE 返回 0 且记录仍是 LOCKED」，四条条件里只有 expire_at
            // 可能不成立——也就是已过期。SYS-1 还没扫到（最长一个扫描周期），但时限是硬约束，
            // 不能因为定时器慢半拍就放行。
            //
            // 但这句提示本身是个**假设**。时钟回拨、DB 与应用时区不一致、或者有人改了 WHERE，
            // 都会让"条件不成立"的原因不是过期。此时用户会看到"已超过确认时限（一个未来时间）"，
            // 现场表现是"系统说我的名额过期了，可截止时间还没到"，而日志里一个字都没有。
            // 所以这里必须留一条 warn：正常的过期是业务分支，不该有日志；
            // 有日志就说明前提被打破了。
            if (record.getExpireAt() != null && !record.getExpireAt().isAfter(now)) {
                log.debug("确认为过期拒绝 resourceId={} studentId={} expireAt={} now={}",
                        resourceId, studentId, record.getExpireAt(), now);
            } else {
                log.warn("confirm 返回 0 但记录仍是 LOCKED 且未过期 —— 条件不成立的原因不是超时 "
                                + "resourceId={} studentId={} expireAt={} now={}，请检查时钟/时区/WHERE 条件",
                        resourceId, studentId, record.getExpireAt(), now);
            }
            return ConfirmOutcome.fail(ErrorCode.RECORD_STATUS_ILLEGAL,
                    "已超过确认时限（" + record.getExpireAt() + "），名额将自动释放，请重新申领");
        }

        // schema 里只有三个状态，出现别的值说明数据被写坏了，不能猜
        log.error("quota_record 出现未知状态 status={} resourceId={} studentId={}",
                status, resourceId, studentId);
        return ConfirmOutcome.fail(ErrorCode.SYSTEM_BUSY);
    }

    private static String nullToDash(String value) {
        return value == null || value.isBlank() ? "-" : value;
    }
}
