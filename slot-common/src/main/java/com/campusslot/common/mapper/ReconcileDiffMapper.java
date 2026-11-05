package com.campusslot.common.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.campusslot.common.entity.ReconcileDiff;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface ReconcileDiffMapper extends BaseMapper<ReconcileDiff> {

    /**
     * 取某活动上一条<b>巡检</b>记录（{@code source='SCAN'}）。
     *
     * <p>只给「连续几个周期仍为负」这条判据用。必须限定 {@code source}：
     * {@code CANCEL} / {@code TIMEOUT} 是事件级记录（释放被拒），它们的方向也是负的
     * （Redis 拒绝回补 → Redis 比 DB 少），混进来会让一次释放事故立刻把某活动的
     * 巡检差异升级成警告，而两者根本不是同一回事。
     *
     * <p>{@code ORDER BY detected_at DESC, id DESC}：同一毫秒连写两条时靠自增 id 定序，
     * 和 {@code QuotaReportMapper#selectLatest} 同一个理由。
     */
    @Select("""
            SELECT * FROM reconcile_diff
             WHERE resource_id = #{resourceId} AND source = 'SCAN'
             ORDER BY detected_at DESC, id DESC
             LIMIT 1
            """)
    ReconcileDiff selectLatestScan(@Param("resourceId") long resourceId);
}
