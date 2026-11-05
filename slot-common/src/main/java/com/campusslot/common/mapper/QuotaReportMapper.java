package com.campusslot.common.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.campusslot.common.entity.QuotaReport;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/** 对账报表。只追加不更新：报表是当时那一刻的结论，事后改数字等于毁掉证据。 */
@Mapper
public interface QuotaReportMapper extends BaseMapper<QuotaReport> {

    /**
     * 取某资源最新一份报表。
     *
     * <p>{@code ORDER BY generated_at DESC, id DESC}：{@code generated_at} 即使升到
     * {@code DATETIME(3)} 仍可能并列（同一毫秒内连出两份，测试里很容易造出来），
     * 只按它排会让"最新一份"不确定。{@code id} 是自增的，天然是写入顺序，作为第二排序键
     * 让结果唯一——不依赖时钟，也不依赖精度。
     */
    @Select("""
            SELECT * FROM quota_report
             WHERE resource_id = #{resourceId}
             ORDER BY generated_at DESC, id DESC
             LIMIT 1
            """)
    QuotaReport selectLatest(@Param("resourceId") long resourceId);

    /**
     * D9 巡检的未平队列。
     *
     * <p>同样加 {@code id DESC}：{@code LIMIT} 截断在并列时会让"返回哪几条"随执行计划变化，
     * 而巡检报告最忌讳的就是"同一份数据跑两次结果不同"。
     */
    @Select("""
            SELECT * FROM quota_report
             WHERE balanced = 0
             ORDER BY generated_at DESC, id DESC
             LIMIT #{limit}
            """)
    List<QuotaReport> selectUnbalanced(@Param("limit") int limit);
}
