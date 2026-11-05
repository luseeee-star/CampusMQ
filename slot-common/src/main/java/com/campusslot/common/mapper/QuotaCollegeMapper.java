package com.campusslot.common.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.campusslot.common.entity.QuotaCollege;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 学院配额。与 {@link QuotaResourceMapper} 同一套「条件更新 + 受影响行数」写法：
 * 扣减带 {@code remain > 0}，回补带 {@code remain < quota}，
 * 保证任何并发下都不会把某个学院的名额扣成负数或回补超过配额。
 */
@Mapper
public interface QuotaCollegeMapper extends BaseMapper<QuotaCollege> {

    @Update("""
            UPDATE quota_college
               SET remain = remain - 1, updated_at = #{now}
             WHERE resource_id = #{resourceId} AND college_code = #{collegeCode} AND remain > 0
            """)
    int deductRemain(@Param("resourceId") long resourceId,
                     @Param("collegeCode") String collegeCode,
                     @Param("now") LocalDateTime now);

    @Update("""
            UPDATE quota_college
               SET remain = remain + 1, updated_at = #{now}
             WHERE resource_id = #{resourceId} AND college_code = #{collegeCode} AND remain < quota
            """)
    int increaseRemain(@Param("resourceId") long resourceId,
                       @Param("collegeCode") String collegeCode,
                       @Param("now") LocalDateTime now);

    @Select("""
            SELECT * FROM quota_college WHERE resource_id = #{resourceId} ORDER BY college_code
            """)
    List<QuotaCollege> listByResource(@Param("resourceId") long resourceId);

    /**
     * 这场发放是否真的配了学院配额。
     *
     * <p>判断依据必须是「表里有没有行」，不是「配额的开关字段」：
     * schema 的口径是「没有行 = 该学院不限额（或不参与）」，所以整场没有行就等于没开配额。
     * 加一个 {@code college_quota_enabled} 字段会让两处口径打架，
     * 表现为「开关是开的，但没有任何学院行」，此时正确的行为恰恰是不拦截。
     */
    @Select("""
            SELECT COUNT(*) FROM quota_college WHERE resource_id = #{resourceId}
            """)
    int countByResource(@Param("resourceId") long resourceId);
}
