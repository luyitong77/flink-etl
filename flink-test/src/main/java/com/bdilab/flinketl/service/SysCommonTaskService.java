package com.bdilab.flinketl.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.bdilab.flinketl.entity.SysCommonTask;
import com.bdilab.flinketl.utils.GlobalResultUtil;

import java.io.Serializable;
import java.util.List;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
public interface SysCommonTaskService extends IService<SysCommonTask>, Serializable {

    /**
     * 建立任务
     * @param taskType
     * @param taskName
     * @return
     */
    GlobalResultUtil<Integer> build(int taskType, String taskName, int parallelism, int userId);

    /**
     * 运行任务
     * @param taskId
     * @return
     */
    GlobalResultUtil<String> run(int taskId);

    /**
     * 停止任务
     * @param taskId
     * @return
     */
    GlobalResultUtil<String> stop(int taskId);

    /**
     * 提交任务
     * @param taskId
     * @param stepOrder
     * @param parallelism
     * @param userId
     * @return
     */
    GlobalResultUtil<Boolean> commitTask(int taskId, String stepOrder, int parallelism, int userId);

    /**
     * 获取普通任务列表
     * @param userId
     * @return
     */
    GlobalResultUtil<List<SysCommonTask>> getCommonTaskList(int userId);
}
