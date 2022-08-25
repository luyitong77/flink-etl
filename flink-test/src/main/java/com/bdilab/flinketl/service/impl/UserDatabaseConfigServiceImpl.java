package com.bdilab.flinketl.service.impl;

import com.bdilab.flinketl.entity.UserDatabaseConfig;
import com.bdilab.flinketl.mapper.UserDatabaseConfigMapper;
import com.bdilab.flinketl.service.UserDatabaseConfigService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-12
 */
@Service
public class UserDatabaseConfigServiceImpl extends ServiceImpl<UserDatabaseConfigMapper, UserDatabaseConfig> implements UserDatabaseConfigService {

    @Resource
    UserDatabaseConfigMapper userDatabaseConfigMapper;

    Logger logger = Logger.getLogger(UserDatabaseConfigServiceImpl.class);

    org.slf4j.Logger logger1 = LoggerFactory.getLogger(UserDatabaseConfigServiceImpl.class);

    @Override
    public boolean saveDatabaseConfig(String configName, int databaseId, int databaseType, String description, int userId) {
        logger.info("-------保存连接连接配置--------");
        logger1.info("-------保存连接连接配置--------");
        UserDatabaseConfig userDatabaseConfig = UserDatabaseConfig.builder()
                .configName(configName)
                .fkDatabaseId(databaseId)
                .databaseType(databaseType)
                .description(description)
                .fkUserId(userId)
                .build();
        return userDatabaseConfigMapper.insert(userDatabaseConfig) > 0;
    }

    @Override
    public boolean deleteUserDatabaseConfigById(int userDatabaseConfigId) {
        return userDatabaseConfigMapper.deleteById(userDatabaseConfigId) > 0;
    }
}
