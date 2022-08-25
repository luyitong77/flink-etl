package com.bdilab.flinketl.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.bdilab.flinketl.entity.DatabaseHive;
import com.bdilab.flinketl.entity.vo.TableStructureVO;
import com.bdilab.flinketl.utils.GlobalResultUtil;
import com.bdilab.flinketl.utils.PageBean;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface DatabaseHiveService extends IService<DatabaseHive> {

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
     * 删除hive中表连接配置
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
     * 获取表结构
     * @param id
     * @param tableName
     * @return
     */

    List<TableStructureVO> getTableStructureInfo(long id, String tableName) throws SQLException;
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
     * 获取该表下所有的数据
     * @param id
     * @param tableName
     * @return
     */
    GlobalResultUtil<List<Map<String, Object>>> getDataPreview(int id, String tableName);


    /**
     * 通过SQL语句建表
     * @param hostname
     * @param port
     * @param username
     * @param password
     * @param sql
     * @return
     */
    GlobalResultUtil<String> createTable(String hostname, int port, String username, String password, String sql);

    /**
     * 保存源数据库配置
     * @param taskId
     * @param dataSourceId
     * @param columns
     * @param columnsType
     * @param tableName
     * @param customSql
     * @return
     */
    GlobalResultUtil<Boolean> saveInput(int taskId, int dataSourceId, String columns, String columnsType, String tableName, String customSql);

    /**
     * 保存目标数据库配置
     * @param taskId
     * @param dataSourceId
     * @param columns
     * @param columnsType
     * @param tableName
     * @param isPartitioned
     * @param partitonCol
     * @param partitonColType
     * @return
     */
    GlobalResultUtil<Boolean> saveOutput(int taskId, int dataSourceId, String columns, String columnsType, String tableName, int isPartitioned, String partitonCol, String partitonColType);

    /**
     *
     * @param id
     * @return
     */
    DatabaseHive getHiveInfo(int id);
}
