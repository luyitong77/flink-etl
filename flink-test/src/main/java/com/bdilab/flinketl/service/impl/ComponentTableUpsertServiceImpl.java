package com.bdilab.flinketl.service.impl;

import com.bdilab.flinketl.entity.ComponentTableUpsert;
import com.bdilab.flinketl.mapper.ComponentTableUpsertMapper;
import com.bdilab.flinketl.service.ComponentTableUpsertService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
@Service
public class ComponentTableUpsertServiceImpl extends ServiceImpl<ComponentTableUpsertMapper, ComponentTableUpsert> implements ComponentTableUpsertService {

    @Resource
    ComponentTableUpsertMapper upsertMapper;

    @Override
    public int saveOutput(int dataSourceId, String columns, String columnsType, String tableName) {
        ComponentTableUpsert upsert = ComponentTableUpsert.builder()
                .fkDataSourceId(dataSourceId)
                .columns(columns)
                .columnsType(columnsType)
                .tableName(tableName)
                .build();
        return upsertMapper.insert(upsert);
    }
}
