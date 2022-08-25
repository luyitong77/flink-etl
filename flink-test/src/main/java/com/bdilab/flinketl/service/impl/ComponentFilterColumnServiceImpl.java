package com.bdilab.flinketl.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bdilab.flinketl.entity.ComponentFilterColumn;
import com.bdilab.flinketl.entity.SysCommonTask;
import com.bdilab.flinketl.mapper.ComponentFilterColumnMapper;
import com.bdilab.flinketl.service.ComponentFilterColumnService;
import com.bdilab.flinketl.service.SysCommonTaskService;
import com.bdilab.flinketl.utils.GlobalResultUtil;
import com.bdilab.flinketl.utils.ResultExecuter;
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
public class ComponentFilterColumnServiceImpl extends ServiceImpl<ComponentFilterColumnMapper, ComponentFilterColumn> implements ComponentFilterColumnService {

    @Resource
    ComponentFilterColumnMapper filterColumnMapper;
    @Resource
    SysCommonTaskService commonTaskService;

    @Override
    public GlobalResultUtil<Boolean> add(int taskId, String column, String type, String function, String condition, int parallelism) {
        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run() throws Exception {
                ComponentFilterColumn filterColumn = ComponentFilterColumn.builder()
                        .columnName(column)
                        .columnType(type)
                        .filterFunction(function)
                        .filterCondition(condition)
                        .parallelism(parallelism)
                        .build();
                filterColumnMapper.insert(filterColumn);

                SysCommonTask commonTask = commonTaskService.getById(taskId);
                commonTask.setFkFilterColumnId(filterColumn.getId());
//                commonTask.setStepOrder(commonTask.getStepOrder()==null ?  WholeVariable.FIELD_FILTERING+" " : commonTask.getStepOrder()+WholeVariable.FIELD_FILTERING+" ");
                return commonTaskService.updateById(commonTask);
            }
        }.execute();
    }
}
