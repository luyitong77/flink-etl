package com.bdilab.flinketl.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.bdilab.flinketl.entity.SysCommonTask;
import org.apache.ibatis.annotations.Param;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
public interface SysCommonTaskMapper extends BaseMapper<SysCommonTask> {

    /**
     * 根据任务id设置状态（用于确认是否完成）
     * @param taskStatus
     * @param jobId
     * @return
     */
    boolean updateCommonTaskByJobId(@Param("taskStatus") int taskStatus, @Param("jobId")String jobId);
}
