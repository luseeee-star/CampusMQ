package com.campusslot.common.constant;

/**
 * 学号的入口校验规则。
 *
 * <p><b>为什么必须在入口拦，而不是交给数据库约束</b>：{@code quota_record.student_id} 是
 * {@code VARCHAR(32)}。超长学号在 DB 层确实插不进去，但那要等到消息被消费时才发生——
 * 而在此之前它已经<b>扣掉了一个 Redis 名额、写了一个幂等键、发了一条注定失败的消息</b>。
 * 最终形态是：名额永久少 1 个、消费者重试到上限后进死信、对账里多一条修真不掉的差异。
 *
 * <p>实测（2026-10-27，D5 前补）：60 字符的学号能一路走到 DB 才报
 * {@code Data too long for column 'student_id'}，而接口返回的是 200「抢到了」。
 * 这违反了需求书 §1 的硬规则：<b>非法输入该在入口被拒，不该让它在扣了名额之后才被其它层拦住。</b>
 *
 * <p>校验分三类，都在 {@link #isValid} 里：
 * <ol>
 *   <li>空 / 全空白 —— 无法定位学生，且会让幂等键退化成 {@code quota:idem:1:} 这种共享 key；
 *       两个人同时提交空学号会互相判成"重放"，一个人抢到、另一个人拿到别人的结果。</li>
 *   <li>长度 &gt; {@link #MAX_LENGTH} —— 见上，DB 会拒，但那时代价已经付了。</li>
 *   <li>含控制字符 —— 会被原样写进日志（换行 = 伪造日志行），也无法安全地放进 URL / Header。</li>
 * </ol>
 *
 * <p>没有做"只允许数字字母"这种白名单：学号格式各校不同（有的带字母、有的带连字符），
 * 收紧到一个猜出来的字符集，代价是某学校的真实学生被整批拒掉，而这在生产上是灾难性的。
 */
public final class StudentIdRule {

    /** 与 {@code deploy/sql/schema.sql} 的 {@code student_id VARCHAR(32)} 一致。改列宽必须同时改这里。 */
    public static final int MAX_LENGTH = 32;

    private StudentIdRule() {
    }

    public static boolean isValid(String studentId) {
        if (studentId == null || studentId.isBlank()) {
            return false;
        }
        if (studentId.length() > MAX_LENGTH) {
            return false;
        }
        for (int i = 0; i < studentId.length(); i++) {
            char c = studentId.charAt(i);
            if (c < 0x20 || c == 0x7F) {
                return false;
            }
        }
        return true;
    }

    /** 给日志 / 错误提示用的说明，说明拒绝的是哪一条规则。 */
    public static String rejectReason(String studentId) {
        if (studentId == null || studentId.isBlank()) {
            return "studentId 不能为空";
        }
        if (studentId.length() > MAX_LENGTH) {
            return "studentId 长度不能超过 " + MAX_LENGTH;
        }
        return "studentId 含非法字符";
    }
}
