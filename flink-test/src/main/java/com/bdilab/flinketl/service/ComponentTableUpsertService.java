package com.bdilab.flinketl.service;

import com.bdilab.flinketl.entity.ComponentTableUpsert;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
public interface ComponentTableUpsertService extends IService<ComponentTableUpsert> {

    /**
     * 保存输出组件配置信息
     * @param columns
     * @param tableName
     * @return
     */
    int saveOutput(int dataSourceId, String columns, String columnsType, String tableName);

}
