package com.bdilab.flinketl.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bdilab.flinketl.entity.*;
import com.bdilab.flinketl.entity.vo.TableStructureVO;
import com.bdilab.flinketl.mapper.DatabaseHiveMapper;
import com.bdilab.flinketl.service.*;
import com.bdilab.flinketl.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class DatabaseHiveServiceImpl extends ServiceImpl<DatabaseHiveMapper, DatabaseHive> implements DatabaseHiveService {

    @Resource
    HiveClient hiveClient;
    @Resource
    DatabaseHiveMapper databaseHiveMapper;
    @Resource
    UserDatabaseConfigService userDatabaseConfigService;
    @Resource
    ComponentTableInputService inputService;
    @Resource
    ComponentTableUpsertService upsertService;
    @Resource
    SysCommonTaskService commonTaskService;

    /**
     * 测试数据库连接
     * @return
     */
    @Override
    public GlobalResultUtil<String> testConnection(String hostname, int port, String username, String password){
        return new ResultExecuter<String>(){
            @Override
            public String run() throws Exception {
                log.info("数据库配置" +"并测试连接Hive---------------");
                DatabaseHive databaseConfig = DatabaseHive.builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .build();

                log.info("--------------成功---------------");
                return hiveClient.testConnection(databaseConfig);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> saveDatabaseConfig(String configName, String description, String databaseName, String hostname, int port, String username, String password, int userId) {
        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run(){
                log.info("数据库配置 : 保存Hive连接---------------");
                DatabaseHive databaseHive = DatabaseHive.builder()
                        .databaseName(databaseName)
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .build();
                databaseHiveMapper.insert(databaseHive);
                int databaseId = databaseHive.getId();

                if (databaseId < 1){
                    return false;
                }

                UserDatabaseConfig databaseConfig = UserDatabaseConfig.builder()
                        .configName(configName)
                        .fkDatabaseId(databaseId)
                        .databaseType(WholeVariable.MYSQL_ID)
                        .description(description)
                        .fkUserId(userId)
                        .build();

               userDatabaseConfigService.save(databaseConfig);
                log.info("--------------成功---------------");
                return true;
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> deleteConnection(int databaseId, int userDatabaseConfigId) {
        return new ResultExecuter<Boolean>() {
            @Override
            public Boolean run() {
                log.info("删除Hive配置---------------");

                boolean isHiveDelete = databaseHiveMapper.deleteById(databaseId) > 0;

                //删除Mysql的连接信息
                boolean isDatabaseConfigDelete = userDatabaseConfigService.deleteUserDatabaseConfigById(userDatabaseConfigId);
                log.info("--------------成功---------------");
                return isHiveDelete && isDatabaseConfigDelete;
            }
        }.execute();
    }


    @Override
    public GlobalResultUtil<List<String>> getAllDatabases(String hostname, int port, String username, String password) {
        return new ResultExecuter<List<String>>() {
            @Override
            public List<String> run() throws SQLException {
                log.info("-------------获取Hive数据库配置" + hostname + "下的所有数据库-------");
                DatabaseHive databaseConfig = DatabaseHive.builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .build();
                return hiveClient.getAllDatabases(databaseConfig);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<PageBean<String>> getAllTables(int id, int currentPage, int pageSize) {
        return new ResultExecuter<PageBean<String>>(){
            @Override
            public PageBean<String> run() throws SQLException{
                log.info("-------------获取Hive数据库配置" + id + "下的数据库的所有表-------");
                List<String> results =  hiveClient.getAllTables(id);
                return SolveReturnResults.returnPageFromResults(results, currentPage, pageSize);
            }
        }.execute();
    }

    /**
     * 获取该数据库中表结构
     *
     * @return
     */
    public List<TableStructureVO> getTableStructureInfo(long id, String tableName) throws SQLException {
        log.info("-------------获取 Hive 数据库:" + id + "下的表:" + tableName + "的表结构信息-------");
        return hiveClient.getTableStructureInfo(id, tableName);
    }

    @Override
    public GlobalResultUtil<List<String>> getAllColumns(int id, String tableName) {
        return new ResultExecuter<List<String>>(){
            @Override
            public List<String> run() throws SQLException {
                log.info("-------------获取Hive数据库配置" + id + "下的数据库的表" +tableName+ "的所有列名-------");
                return hiveClient.getAllColumns(id, tableName);
            }
        }.execute();
    }
    @Override
    public GlobalResultUtil<List<Map<String, Object>>> getDataPreview(int id, String tableName) {
        return new ResultExecuter<List<Map<String, Object>>>(){
            @Override
            public List<Map<String, Object>> run() throws SQLException {
                log.info("-------------获取Hive数据库配置" + id + "下的数据库的表" +tableName+ "的所有数据-------");
                return hiveClient.getDataPreview(id, tableName);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<String> createTable(String hostname, int port, String username, String password, String sql) {
        return new ResultExecuter<String>(){
            @Override
            public String run() throws Exception {
                log.info("数据库配置" +"---------------");
                DatabaseHive databaseConfig = DatabaseHive.builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .build();

                log.info("--------------成功---------------");
                return hiveClient.createTable(databaseConfig,sql);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> saveInput(int taskId, int dataSourceId, String columns, String columnsType, String tableName, String customSql) {
        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run() {
                log.info("-------------保存源数据库配置-------");
                ComponentTableInput input;
                if (customSql != null) {
                    input = ComponentTableInput.builder()
                            .fkDataSourceId(dataSourceId)
                            .customSql(customSql)
                            .build();
                } else {
                    input = ComponentTableInput.builder()
                            .fkDataSourceId(dataSourceId)
                            .tableName(tableName)
                            .columns(columns)
                            .columnsType(columnsType)
                            .build();
                }
                inputService.save(input);
                SysCommonTask commonTask = SysCommonTask.builder()
                        .id(taskId)
                        .fkInputId(input.getId())
                        .sourceDataType(WholeVariable.HIVE)
                        .build();
                return commonTaskService.updateById(commonTask);
            }
        }.execute();
    }

    public GlobalResultUtil<Boolean> saveOutput(int taskId, int dataSourceId, String columns, String columnsType, String tableName, int isPartitioned, String partitonCol, String partitonColType) {
        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run() {
                log.info("-------------保存目标数据库配置-------");
//                int upsertId = upsertService.saveOutput(dataSourceId, columns, columnsType, tableName);

                ComponentTableUpsert upsert = ComponentTableUpsert.builder()
                        .fkDataSourceId(dataSourceId)
                        .tableName(tableName)
                        .columns(columns)
                        .columnsType(columnsType)
                        .isPartitioned(1)
                        .partitonCol(partitonCol)
                        .partitiionColType(partitonColType)
                        .build();

                upsertService.save(upsert);

                SysCommonTask commonTask = SysCommonTask.builder()
                        .id(taskId)
                        .fkUpsertId(upsert.getId())
                        .targetDataType(WholeVariable.HIVE)
                        .build();
                return commonTaskService.updateById(commonTask);
            }
        }.execute();
    }

    @Override
    public DatabaseHive getHiveInfo(int id) {
        return databaseHiveMapper.selectById(id);
    }
}
