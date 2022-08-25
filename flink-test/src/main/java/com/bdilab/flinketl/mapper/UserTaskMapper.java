package com.bdilab.flinketl.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.bdilab.flinketl.entity.UserTask;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author hcyong
 * @since 2021-07-16
 */
public interface UserTaskMapper extends BaseMapper<UserTask> {

    /**
     * 根据用户id获取用户任务列表
     * @param userId
     * @return
     */
    List<UserTask> findUserTaskByUserId(@Param("userId") int userId);
}
