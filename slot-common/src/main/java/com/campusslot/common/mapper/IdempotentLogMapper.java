package com.campusslot.common.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.campusslot.common.entity.IdempotentLog;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface IdempotentLogMapper extends BaseMapper<IdempotentLog> {
}
