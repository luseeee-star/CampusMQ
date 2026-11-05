package com.campusslot.app.resource.dto;

import java.util.List;

/**
 * 发布结果。
 *
 * <p>{@code onlyIfAbsent} 直接回显这次走的是哪条分支（覆盖 / 只补缺），
 * 因为两种分支的"成功"在外部看起来一样，而它们的故障模式完全不同：
 * <ul>
 *   <li>覆盖写错了 → 余量被回滚成旧值（观众看到"多了几个名额"）</li>
 *   <li>只补缺写错了 → 缺的 key 永远补不上（观众看到"全场系统繁忙"）</li>
 * </ul>
 * 回显一个布尔值，比在日志里翻"这次是哪个 if"便宜得多。
 */
public record PublishResult(
        long resourceId,
        String beforeStatus,
        String afterStatus,
        boolean statusMigrated,
        boolean onlyIfAbsent,
        int stockRemain,
        int collegeRows,
        int collegeKeysWritten,
        int rosterRows,
        int rosterKeysWritten,
        List<String> notes
) {
}
