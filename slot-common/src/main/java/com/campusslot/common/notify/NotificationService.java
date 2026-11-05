package com.campusslot.common.notify;

import com.campusslot.common.constant.AppTime;
import com.campusslot.common.entity.Notification;
import com.campusslot.common.mapper.NotificationMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 写通知记录（SYS-2 的"入账"半边，不作任何网络发送）。
 *
 * <p><b>不加重试逻辑、不开新事务。</b>调用方必须已经在自己的事务里：
 * 确认是一次 UPDATE + 一次 INSERT，两条写要么一起成立、要么一起回滚——
 * 否则会出现"名额已 CONFIRMED 但通知记录不存在"，而这条缺口事后没有任何地方能补
 * （通知是事件，不是状态，D9 对账能对齐余量，对齐不了"该发没发"）。
 *
 * <p>不用 {@code @Transactional(REQUIRES_NEW)}：那会让通知提交在业务之前/之后独立成败，
 * 等于把问题从"一起失败"换成"两张表不一致"。唯一正确的做法是同一个事务。
 *
 * <p><b>写入失败时抛异常</b>，让调用方的事务回滚。确认路径有 replay 分支（学生再点一次
 * 就是一次完整的重试），取消路径同理；而"静默吞掉通知写入失败"换来的是永久丢失。
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    /**
     * 当前只有 MOCK 通道（SYS-2 明确不接真实短信/企微）。
     * 通道进唯一键，所以将来接第二个通道时，同一条业务事件会有两行、各自重试——这是刻意的。
     */
    public static final String CHANNEL_MOCK = "MOCK";

    private static final String STATUS_PENDING = "PENDING";

    private final NotificationMapper mapper;

    public NotificationService(NotificationMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * 入账一条待发通知。
     *
     * @return true 表示新建；false 表示已经存在同 {@code (bizId, channel)} 的行（重复点击、消息重投），
     *         此时<b>什么都不改</b>——尤其不能把已经 DEAD 的通知重置回 PENDING
     */
    public boolean enqueue(NotifyEvent event, long resourceId, String studentId) {
        Notification n = new Notification();
        n.setBizId(event.bizId(resourceId, studentId));
        n.setStudentId(studentId);
        n.setResourceId(resourceId);
        n.setChannel(CHANNEL_MOCK);
        n.setTitle(event.title());
        n.setContent(event.content());
        n.setStatus(STATUS_PENDING);
        n.setRetryCount(0);
        // created_at 由应用时钟写入：列是 NOT NULL，显式传 NULL 不会回落到 DEFAULT CURRENT_TIMESTAMP
        n.setCreatedAt(AppTime.now());

        int inserted = mapper.insertIgnore(n);
        if (inserted == 0) {
            log.debug("通知已存在，跳过 event={} resourceId={} studentId={}",
                    event, resourceId, studentId);
            return false;
        }
        return true;
    }
}
