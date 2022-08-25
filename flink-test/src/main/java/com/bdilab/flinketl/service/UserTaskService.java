package com.bdilab.flinketl.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.bdilab.flinketl.entity.UserTask;

import java.util.List;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-16
 */
public interface UserTaskService extends IService<UserTask> {


    /**
     * 根据用户id获取任务id
     * @param userId
     * @return
     */
    List<UserTask> getUserTaskByUserId(int userId);
}
