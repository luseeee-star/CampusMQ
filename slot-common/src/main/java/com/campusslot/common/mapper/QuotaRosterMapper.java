package com.campusslot.common.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * 资格名单。取消路径只用到一件事：<b>这个学生属于哪个学院</b>。
 *
 * <p><b>为什么取消时不从 Redis 的 {@code quota:roster:{rid}:{sid}} 读学院编码</b>：
 * 那个 key 的 TTL 是 10 分钟，活动进行中也可能被淘汰。取不到就会静默走到「不回补学院余量」
 * 的分支——学院配额永久少 1，且没有任何日志。DB 里的 {@code quota_roster.college_code}
 * 才是权威值，一次主键级查询的代价在取消路径上完全可以接受（量级比抢号低三个数量级）。
 *
 * <p>不继承 {@code BaseMapper}，也不建实体：这张表在本项目里只有这一处读取，
 * 为了一个 {@code String} 造一个九字段实体，往后每个看代码的人都要多翻一个文件。
 */
@Mapper
public interface QuotaRosterMapper {

    /** 学生不在名单里、或名单行没填学院时返回 null——两者对调用方是同一个含义：不回补学院余量。 */
    @Select("""
            SELECT college_code FROM quota_roster
             WHERE resource_id = #{resourceId} AND student_id = #{studentId}
            """)
    String selectCollegeCode(@Param("resourceId") long resourceId,
                             @Param("studentId") String studentId);
}
