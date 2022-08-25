package com.bdilab.flinketl.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.bdilab.flinketl.entity.DatabaseKafka;
import com.bdilab.flinketl.entity.DatabaseOracle;
import com.bdilab.flinketl.utils.GlobalResultUtil;
import com.bdilab.flinketl.utils.PageBean;

import java.util.List;

/**
 * <p>
 * 数据库配置表 服务类
 * </p>
 *
 * @author ljw
 * @since 2021-07-28
 */
public interface DatabaseKafkaService extends IService<DatabaseKafka> {

    /**
     * 测试kafka连接
     * @param hostname
     * @param port
     * @param databaseName
     * @param username
     * @param password
     * @param isServiceName
     * @return
     */
    GlobalResultUtil<String> testConnection(String hostname, int port, String databaseName, String username, String password, int isServiceName);


    /**
     * 保存kafka连接配置
     * @param configName
     * @param description
     * @param databaseName
     * @param hostname
     * @param port
     * @param username
     * @param password
     * @param isServiceName
     * @param userId
     * @return
     */
    GlobalResultUtil<Boolean> saveDatabaseConfig(String configName, String description, String databaseName, String hostname, int port, String username, String password, int isServiceName, int userId);

    /**
     * 删除kafka中表连接配置
     * @param databaseId
     * @param userDatabaseConfigId
     * @return
     */
    GlobalResultUtil<Boolean> deleteConnection(int databaseId, int userDatabaseConfigId);

    /**
     * 获取kafka所有的模式
     * @param hostname
     * @param port
     * @param databaseName
     * @param username
     * @param password
     * @param isServiceName
     * @return
     */
    GlobalResultUtil<List<String>> getALlSchemas(String hostname, int port, String databaseName, String username, String password, int isServiceName);

    /**
     * 获取kafka数据库下的所有的表
     * @param id
     * @param currentPage
     * @param pageSize
     * @return
     */
    GlobalResultUtil<PageBean<String>> getAllTables(long id, int currentPage, int pageSize);

    /**
     * 获取该表下所有的列名
     * @param id
     * @param tableName
     * @return
     */
    GlobalResultUtil<List<String>> getAllColumns(long id, String tableName);

    /**
     * 保存源数据库配置
     * @param columns
     * @param tableName
     * @return
     */
    GlobalResultUtil<Boolean> saveInput(int taskId, int dataSourceId, String columns, String columnsType, String tableName);

    /**
     * 保存目标数据库配置
     * @param taskId
     * @param columns
     * @param tableName
     * @return
     */
    GlobalResultUtil<Boolean> saveOutput(int taskId, int dataSourceId, String columns, String columnsType, String tableName);
}
