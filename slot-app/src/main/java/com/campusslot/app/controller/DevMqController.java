package com.campusslot.app.controller;

import com.campusslot.common.api.ErrorCode;
import com.campusslot.common.api.BizException;
import com.campusslot.common.api.R;
import com.campusslot.common.constant.AppTime;
import com.campusslot.common.constant.MqTopic;
import com.campusslot.common.mq.MqProducer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * D1 三个验证点的工具端点（dev 专用，试点上线前必须关掉）。
 *
 * <p>用它回答三个只能用代码验证、不能靠文档断言的问题：
 * <ol>
 *   <li><b>5.x 任意时间定时消息是否真的生效</b>：发一条 37 秒后投递的消息，
 *       看 {@code slot-consumer} 日志里的消费时刻——4.x 客户端凑不出 37 秒这个档位，
 *       如果消息 30 秒就到了，说明客户端行为与预期不符，D5 才发现就是灾难</li>
 *   <li><b>Topic 队列数是否真的生效</b>：见 deploy/topic-init.sh 之后的 mqadmin topicStatus</li>
 *   <li><b>链路是否通</b>：能不能发出去、消费者能不能收到</li>
 * </ol>
 */
@RestController
@RequestMapping("/api/dev/mq")
@ConditionalOnProperty(prefix = "campusslot", name = "dev-endpoints", havingValue = "true")
public class DevMqController {

    private final MqProducer producer;

    public DevMqController(MqProducer producer) {
        this.producer = producer;
    }

    /** 立即投递一条。 */
    @PostMapping("/send")
    public R<Map<String, Object>> send(@RequestParam(defaultValue = "hello") String body,
                                       @RequestParam(defaultValue = "smoke") String key) {
        long start = System.currentTimeMillis();
        String messageId = producer.send(MqTopic.QUOTA_CLAIMED, key, "smoke", body);
        return R.ok(payload(messageId, start, 0));
    }

    /**
     * 延时投递。故意取 37 秒这种「非 4.x 档位」的值——
     * 如果它被归到 30 秒档，说明走的是老客户端路径，任意时刻定时能力实际没拿到。
     */
    @PostMapping("/send-delay")
    public R<Map<String, Object>> sendDelay(@RequestParam(defaultValue = "37") long delaySeconds,
                                            @RequestParam(defaultValue = "delay-test") String key,
                                            @RequestParam(defaultValue = "delayed-hello") String body) {
        if (delaySeconds <= 0 || delaySeconds > 24 * 3600) {
            throw new BizException(ErrorCode.PARAM_INVALID, "delaySeconds 需在 1..86400 之间");
        }
        long start = System.currentTimeMillis();
        String messageId = producer.sendDelay(MqTopic.QUOTA_CLAIMED, key, "delay",
                body, java.time.Duration.ofSeconds(delaySeconds));
        return R.ok(payload(messageId, start, delaySeconds));
    }

    @GetMapping("/info")
    public R<Map<String, Object>> info() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("topics", Map.of(
                MqTopic.QUOTA_CLAIMED, MqTopic.QUOTA_CLAIMED_QUEUES + " queues",
                MqTopic.QUOTA_RELEASE_DELAY, MqTopic.QUOTA_RELEASE_DELAY_QUEUES + " queues",
                MqTopic.QUOTA_BACKFILL, MqTopic.QUOTA_BACKFILL_QUEUES + " queues"));
        map.put("expect", "延时消息到达时间偏差应记录到 LOGBOOK，不要引用组件文档的数字");
        return R.ok(map);
    }

    private Map<String, Object> payload(String messageId, long start, long delaySeconds) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("messageId", messageId);
        map.put("sentAt", AppTime.now().toString());
        map.put("sendCostMs", System.currentTimeMillis() - start);
        map.put("delaySeconds", delaySeconds);
        if (messageId != null && messageId.startsWith("noop-")) {
            map.put("warning", "MQ 未启用，消息没有真的进 Broker（campusslot.mq.enabled=false）");
        }
        return map;
    }
}
