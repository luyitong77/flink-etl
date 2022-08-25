package com.bdilab.flinketl.service;

import com.bdilab.flinketl.entity.ComponentFilterColumn;
import com.baomidou.mybatisplus.extension.service.IService;
import com.bdilab.flinketl.utils.GlobalResultUtil;

import java.io.Serializable;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
public interface ComponentFilterColumnService extends IService<ComponentFilterColumn>, Serializable {

    /**
     * 添加过滤组件
     * @param taskId
     * @param column
     * @param type
     * @param function
     * @param condition
     * @return
     */
    GlobalResultUtil<Boolean> add(int taskId, String column, String type, String function, String condition, int parallelism);

}
