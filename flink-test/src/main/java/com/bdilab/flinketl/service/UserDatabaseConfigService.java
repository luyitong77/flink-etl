package com.bdilab.flinketl.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.bdilab.flinketl.entity.UserDatabaseConfig;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-12
 */
public interface UserDatabaseConfigService extends IService<UserDatabaseConfig> {

    /**
     * 保存用户的数据库配置信息
     * @param configName
     * @param databaseId
     * @param databaseType
     * @param description
     * @param userId
     * @return
     */
    boolean saveDatabaseConfig(String configName, int databaseId, int databaseType, String description, int userId);

    /**
     * 删除指定的信息表中信息
     *
     * @param userDatabaseConfigId
     * @return
     */
    boolean deleteUserDatabaseConfigById(int userDatabaseConfigId);
}
