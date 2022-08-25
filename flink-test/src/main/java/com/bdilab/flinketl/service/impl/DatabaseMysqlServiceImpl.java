package com.bdilab.flinketl.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bdilab.flinketl.entity.*;
import com.bdilab.flinketl.mapper.DatabaseMysqlMapper;
import com.bdilab.flinketl.service.*;
import com.bdilab.flinketl.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.sql.SQLException;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
@Slf4j
@Service
public class DatabaseMysqlServiceImpl extends ServiceImpl<DatabaseMysqlMapper, DatabaseMysql> implements DatabaseMysqlService {

    @Resource
    MySqlClient mySqlClient;
    @Resource
    DatabaseMysqlMapper databaseMysqlMapper;
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
                log.info("数据库配置" +"并测试连接Mysql---------------");
                DatabaseMysql databaseConfig = DatabaseMysql.builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .build();

                log.info("--------------成功---------------");
                return mySqlClient.testConnection(databaseConfig);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> saveDatabaseConfig(String configName, String description, String databaseName, String hostname, int port, String username, String password, int userId) {
        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run(){
                log.info("数据库配置 : 保存Mysql连接---------------");
                DatabaseMysql databaseMysql = DatabaseMysql.builder()
                        .databaseName(databaseName)
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .build();
                databaseMysqlMapper.insert(databaseMysql);
                int databaseId = databaseMysql.getId();

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
//                userDatabaseConfigService.saveDatabaseConfig(configName, databaseId, WholeVariable.MYSQL_ID, description, userId);
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
                log.info("删除Mysql配置---------------");

                boolean isMysqlDelete = databaseMysqlMapper.deleteById(databaseId) > 0;

                //删除Mysql的连接信息
                boolean isDatabaseConfigDelete = userDatabaseConfigService.deleteUserDatabaseConfigById(userDatabaseConfigId);
                log.info("--------------成功---------------");
                return isMysqlDelete && isDatabaseConfigDelete;
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<List<String>> getAllDatabases(String hostname, int port, String username, String password) {
        return new ResultExecuter<List<String>>() {
            @Override
            public List<String> run() throws SQLException {
                log.info("-------------获取Mysql数据库配置" + hostname + "下的所有数据库-------");
                DatabaseMysql databaseMysql = DatabaseMysql.builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .build();
                return mySqlClient.getAllDatabases(databaseMysql);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<PageBean<String>> getAllTables(int id, int currentPage, int pageSize) {
        return new ResultExecuter<PageBean<String>>(){
            @Override
            public PageBean<String> run() throws SQLException{
                log.info("-------------获取Mysql数据库配置" + id + "下的数据库的所有表-------");

                List<String> results =  mySqlClient.getAllTables(id);
                return SolveReturnResults.returnPageFromResults(results, currentPage, pageSize);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<List<String>> getAllColumns(int id, String tableName) {
        return new ResultExecuter<List<String>>(){
            @Override
            public List<String> run() throws SQLException {
                log.info("-------------获取Mysql数据库配置" + id + "下的数据库的表" +tableName+ "的所有列名-------");
                return mySqlClient.getAllColumns(id, tableName);
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

//                int inputId = inputService.saveInput(dataSourceId, columns, columnsType, tableName);
                inputService.save(input);
                SysCommonTask commonTask = SysCommonTask.builder()
                        .id(taskId)
                        .fkInputId(input.getId())
                        .sourceDataType(WholeVariable.MYSQL)
                        .build();
                return commonTaskService.updateById(commonTask);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> saveOutput(int taskId, int dataSourceId, String columns, String columnsType, String tableName) {
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
                        .build();

                upsertService.save(upsert);

                SysCommonTask commonTask = SysCommonTask.builder()
                        .id(taskId)
                        .fkUpsertId(upsert.getId())
                        .targetDataType(WholeVariable.MYSQL)
                        .build();
                return commonTaskService.updateById(commonTask);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> checkSql(int dataSourceId, String sql) {
        return new ResultExecuter<Boolean>() {
            @Override
            public Boolean run() {
                log.info("开始校验自定义sql查询: " + sql);
                return mySqlClient.checkSql(dataSourceId, sql);
            }
        }.execute();
    }
}
