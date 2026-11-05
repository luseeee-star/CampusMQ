package com.campusslot.common.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 接口级幂等日志。第一层幂等键落 Redis，DB 这份用于事后审计与 Redis 抖动后的兜底核查。
 *
 * <p>R-3 要求「重复请求返回首次请求的结果」，所以这张表必须记下首次产生的 {@code record_id} 与结果码，
 * 而不是只记「这个 key 来过」。只记来过的实现，最后只能回一句「你已参与过」——那正是 §4.2 表格里
 * 明确要避免的文案。
 */
@Data
@TableName("idempotent_log")
public class IdempotentLog {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String idempotentKey;

    private String studentId;

    private Long resourceId;

    /** 首次请求返回的结果码，重放时直接复用 */
    private String resultCode;

    /** 首次请求产生的申领记录 ID，重放时用它复现「已锁定 + 确认倒计时」 */
    private Long recordId;

    private LocalDateTime createdAt;
}
