package com.bdilab.flinketl.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bdilab.flinketl.entity.ComponentTableInput;
import com.bdilab.flinketl.mapper.ComponentTableInputMapper;
import com.bdilab.flinketl.service.ComponentTableInputService;
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
public class ComponentTableInputServiceImpl extends ServiceImpl<ComponentTableInputMapper, ComponentTableInput> implements ComponentTableInputService {

    @Resource
    ComponentTableInputMapper inputMapper;

    @Override
    public int saveInput(int dataSourceId, String columns, String columnsType, String tableName) {
        ComponentTableInput input = ComponentTableInput.builder()
                .fkDataSourceId(dataSourceId)
                .columns(columns)
                .columnsType(columnsType)
                .tableName(tableName)
                .build();
        return inputMapper.insert(input);
    }
}
