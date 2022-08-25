package com.bdilab.flinketl.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.bdilab.flinketl.entity.DatabaseMysql;
import com.bdilab.flinketl.utils.GlobalResultUtil;
import com.bdilab.flinketl.utils.PageBean;

import java.util.List;
import java.util.Map;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
public interface DatabaseMysqlService extends IService<DatabaseMysql> {

    /**
     * 测试连接
     * @param hostname
     * @param port
     * @param username
     * @param password
     * @return
     */
    GlobalResultUtil<String> testConnection(String hostname, int port, String username, String password);


    /**
     * 保存连接配置
     * @param configName
     * @param description
     * @param databaseName
     * @param hostname
     * @param port
     * @param username
     * @param password
     * @param userId
     * @return
     */
    GlobalResultUtil<Boolean> saveDatabaseConfig(String configName, String description, String databaseName, String hostname, int port, String username, String password, int userId);

    /**
     * 删除mysql中表连接配置
     * @param databaseId
     * @param userDatabaseConfigId
     * @return
     */
    GlobalResultUtil<Boolean> deleteConnection(int databaseId, int userDatabaseConfigId);

    /**
     * 获取所有的数据库
     * @param hostname
     * @param port
     * @param username
     * @param password
     * @return
     */
    GlobalResultUtil<List<String>> getAllDatabases(String hostname, int port, String username, String password);

    /**
     * 获取该数据库下的所有的表
     * @param id
     * @param currentPage
     * @param pageSize
     * @return
     */
    GlobalResultUtil<PageBean<String>> getAllTables(int id, int currentPage, int pageSize);

    /**
     * 获取该表下所有的列名
     * @param id
     * @param tableName
     * @return
     */
    GlobalResultUtil<List<String>> getAllColumns(int id, String tableName);

    /**
     * 保存源数据库配置
     * @param columns
     * @param tableName
     * @return
     */
    GlobalResultUtil<Boolean> saveInput(int taskId, int dataSourceId, String columns, String columnsType, String tableName, String customSql);

    /**
     * 保存目标数据库配置
     * @param taskId
     * @param columns
     * @param tableName
     * @return
     */
    GlobalResultUtil<Boolean> saveOutput(int taskId, int dataSourceId, String columns, String columnsType, String tableName);

    /**
     * 检验用户自定义sql查询语句
     * @param dataSourceId
     * @param sql
     * @return
     */
    GlobalResultUtil<Boolean> checkSql(int dataSourceId, String sql);
}
