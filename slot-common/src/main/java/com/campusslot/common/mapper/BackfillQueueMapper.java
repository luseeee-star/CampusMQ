package com.campusslot.common.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.campusslot.common.entity.BackfillQueue;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface BackfillQueueMapper extends BaseMapper<BackfillQueue> {
}
