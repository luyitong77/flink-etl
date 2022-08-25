package com.bdilab.flinketl.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bdilab.flinketl.entity.ComponentSplitColumn;
import com.bdilab.flinketl.entity.SysCommonTask;
import com.bdilab.flinketl.mapper.ComponentSplitColumnMapper;
import com.bdilab.flinketl.service.ComponentSplitColumnService;
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
 * @since 2021-07-22
 */
@Service
public class ComponentSplitColumnServiceImpl extends ServiceImpl<ComponentSplitColumnMapper, ComponentSplitColumn> implements ComponentSplitColumnService {

    @Resource
    ComponentSplitColumnMapper splitColumnMapper;
    @Resource
    SysCommonTaskService commonTaskService;

    @Override
    public GlobalResultUtil<Boolean> add(int taskId, String delimiter, String splitColumnName, String saveColumnName, String saveColumnType, int parallelism) {
        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run() throws Exception {
                ComponentSplitColumn splitColumn = ComponentSplitColumn.builder()
                        .delimiter(delimiter)
                        .splitColumnName(splitColumnName)
                        .saveColumnName(saveColumnName)
                        .saveColumnType(saveColumnType)
                        .parallelism(parallelism)
                        .build();

                splitColumnMapper.insert(splitColumn);

                SysCommonTask commonTask = commonTaskService.getById(taskId);
                commonTask.setFkSplitColumnId(splitColumn.getId());
//                commonTask.setStepOrder(commonTask.getStepOrder()==null ?  WholeVariable.FIELD_FILTERING+" " : commonTask.getStepOrder()+WholeVariable.FIELD_FILTERING+" ");
                return commonTaskService.updateById(commonTask);
            }
        }.execute();
    }
}
