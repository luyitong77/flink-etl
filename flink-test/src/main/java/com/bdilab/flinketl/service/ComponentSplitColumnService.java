package com.bdilab.flinketl.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.bdilab.flinketl.entity.ComponentSplitColumn;
import com.bdilab.flinketl.utils.GlobalResultUtil;

import java.io.Serializable;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-22
 */
public interface ComponentSplitColumnService extends IService<ComponentSplitColumn>, Serializable {

    /**
     * 添加分割组件
     * @param taskId
     * @param delimiter
     * @param splitColumnName
     * @param saveColumnName
     * @param saveColumnType
     * @param parallelism
     * @return
     */
    GlobalResultUtil<Boolean> add(int taskId, String delimiter, String splitColumnName, String saveColumnName, String saveColumnType, int parallelism);


}
