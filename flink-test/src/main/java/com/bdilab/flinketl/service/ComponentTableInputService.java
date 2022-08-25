package com.bdilab.flinketl.service;

import com.bdilab.flinketl.entity.ComponentTableInput;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
public interface ComponentTableInputService extends IService<ComponentTableInput> {

    /**
     * 保存输入组件配置信息
     * @param columns
     * @param tableName
     * @return
     */
    int saveInput(int dataSourceId, String columns, String columnsType, String tableName);

}
