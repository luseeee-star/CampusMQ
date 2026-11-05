package com.campusslot.app.resource.dto;

/**
 * 一个学院的三本账并列。
 *
 * <p><b>两套口径不要混成第三个数</b>：
 * <ul>
 *   <li>{@code quota / remainDb / remainRedis} —— <b>配置口径</b>，来自 {@code quota_college}，
 *       是"这个学院最多给几个"；</li>
 *   <li>{@code issued / locked / confirmed} —— <b>实际口径</b>，来自 {@code quota_record}，
 *       是"这个学院实际领走几个"。</li>
 * </ul>
 * 两个数天然可以不相等：活动没发满、或者该学院没有名额上限（只有名单不带配额）。
 * 把它们的差当成"错误"是最常见的误读。
 *
 * <p><b>{@code collegeCode == null} 是一个真实的桶，不是空值噪音</b>：
 * 实际口径按 {@code quota_record LEFT JOIN quota_roster} 分组，名单里没有这个学生
 * （活动的 {@code roster_required = 0}，或者记录是手工插的）就 join 不上，编码为 null。
 * 这个桶必须显示成「未分学院」并参与合计，否则各学院加起来对不上总数——
 * 看起来像丢了几条记录。
 *
 * @param configured {@code quota_college} 里有没有这一行。false 时 quota/remain 均为 null，
 *                   只有实际口径，别把 null 读成 0 个名额
 * @param drift      该学院 {@code redisRemain − remainDb}；key 不存在时为 null
 * @param rosterRows 该学院名单上的人数（分母）。和 issued 一起看才知道是"没人抢"还是"配额卡住"
 */
public record CollegeProgress(
        String collegeCode,
        String collegeLabel,
        boolean configured,
        Integer quota,
        Integer remainDb,
        Integer remainRedis,
        Integer drift,
        int rosterRows,
        int locked,
        int confirmed,
        int issued,
        int released
) {
}
