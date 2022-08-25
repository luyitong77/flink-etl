package com.bdilab.flinketl.service.impl;

import com.bdilab.flinketl.entity.UserTask;
import com.bdilab.flinketl.mapper.UserTaskMapper;
import com.bdilab.flinketl.service.UserTaskService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-16
 */
@Service
public class UserTaskServiceImpl extends ServiceImpl<UserTaskMapper, UserTask> implements UserTaskService {

    @Resource
    UserTaskMapper userTaskMapper;

    @Override
    public List<UserTask> getUserTaskByUserId(int userId) {
        return userTaskMapper.findUserTaskByUserId(userId);
    }
}
