package com.bdilab.flinketl.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bdilab.flinketl.entity.*;
import com.bdilab.flinketl.mapper.DatabaseSqlServerMapper;
import com.bdilab.flinketl.service.*;
import com.bdilab.flinketl.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.sql.SQLException;
import java.util.List;

@Slf4j
@Service
public class DatabaseSqlServerServiceImpl extends ServiceImpl<DatabaseSqlServerMapper, DatabaseSqlserver> implements DatabaseSqlServerService {
    @Resource
    SqlServerClient sqlServerClient;
    @Resource
    DatabaseSqlServerMapper databaseSqlserverMapper;
    @Resource
    UserDatabaseConfigService userDatabaseConfigService;
    @Resource
    ComponentTableInputService inputService;
    @Resource
    ComponentTableUpsertService upsertService;
    @Resource
    SysCommonTaskService commonTaskService;


    Logger logger = Logger.getLogger(DatabaseSqlServerServiceImpl.class);

    org.slf4j.Logger logger1 = LoggerFactory.getLogger(DatabaseSqlServerServiceImpl.class);

    /**
     * 测试数据库连接
     * @return
     */
    @Override
    public GlobalResultUtil<String> testConnection(String hostname, int port, String username, String password){
        return new ResultExecuter<String>(){
            @Override
            public String run() throws Exception {
                logger.info("数据库配置" +"并测试连接SqlServer---------------\n");
                logger1.info("数据库配置" +"并测试连接---------------");
                //这里目前使用的DatabaseMysql对象（但这个对象应该对所有的数据库都匹配，应该提升抽象度）
                DatabaseSqlserver databaseConfig = DatabaseSqlserver.builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .build();
                logger.info("--------------成功---------------");
                return sqlServerClient.testConnection(databaseConfig);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> saveDatabaseConfig(String configName, String description, String databaseName, String hostname, int port, String username, String password, int userId) {
        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run(){
                logger.info("数据库配置 : 保存SqlServer连接---------------");
                logger1.info("数据库配置 : 保存SqlServer连接到mysql数据库中---------------");
                DatabaseSqlserver databaseConfig = DatabaseSqlserver.builder()
                        .databaseName(databaseName)
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .build();
                databaseSqlserverMapper.insert(databaseConfig);
                int databaseId = databaseConfig.getId();

                if (databaseId < 1){
                    return false;
                }

                UserDatabaseConfig userDatabaseConfig = UserDatabaseConfig.builder()
                        .configName(configName)
                        .fkDatabaseId(databaseId)
                        .databaseType(WholeVariable.SQLSERVER_ID)
                        .description(description)
                        .fkUserId(userId)
                        .build();
//                userDatabaseConfigService.saveDatabaseConfig(configName, databaseId, WholeVariable.MYSQL_ID, description, userId);
                userDatabaseConfigService.save(userDatabaseConfig);
                logger.info("--------------成功---------------");
                return true;
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> deleteConnection(int databaseId, int userDatabaseConfigId) {
        return new ResultExecuter<Boolean>() {
            @Override
            public Boolean run() {
                logger.info("删除SqlServer配置---------------");
                logger1.info("删除SqlServe连接配置---------------");

                boolean isSqlServerDelete = databaseSqlserverMapper.deleteById(databaseId) > 0;

                //删除SqlServer的连接信息
                boolean isDatabaseConfigDelete = userDatabaseConfigService.deleteUserDatabaseConfigById(userDatabaseConfigId);
                logger.info("--------------成功---------------");
                return isSqlServerDelete && isDatabaseConfigDelete;
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<List<String>> getAllDatabases(String hostname, int port, String username, String password) {
        return new ResultExecuter<List<String>>() {
            @Override
            public List<String> run() throws SQLException {
                logger.info("-------------获取SqlServer数据库配置" + hostname + "下的所有数据库-------");
                logger1.info("-------------获取SqlServer数据库配置" + hostname + "下的所有数据库-------");
                DatabaseSqlserver databaseConfig = DatabaseSqlserver.builder()
                        .hostname(hostname)
                        .port(port)
                        .username(username)
                        .password(password)
                        .build();
                return sqlServerClient.getAllDatabases(databaseConfig);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<PageBean<String>> getAllTables(int id, int currentPage, int pageSize) {
        return new ResultExecuter<PageBean<String>>(){
            @Override
            public PageBean<String> run() throws SQLException{
                logger.info("-------------获取SqlServer数据库配置" + id + "下的数据库的所有表-------");
                logger1.info("-------------获取SqlServe数据库配置" + id + "下的数据库的所有表-------");

                List<String> results =  sqlServerClient.getAllTables(id);
                return SolveReturnResults.returnPageFromResults(results, currentPage, pageSize);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<List<String>> getAllColumns(int id, String tableName) {
        return new ResultExecuter<List<String>>(){
            @Override
            public List<String> run() throws SQLException {
                logger.info("-------------获取SqlServer数据库配置" + id + "下的数据库的表" +tableName+ "的所有列名-------");
                logger1.info("-------------获取SqlServer数据库配置" + id + "下的数据库的表" + tableName+ "的所有列名-------");
                return sqlServerClient.getAllColumns(id, tableName);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> saveInput(int taskId, int dataSourceId, String columns, String columnsType, String tableName) {
        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run() throws SQLException {
                logger.info("-------------保存源数据库配置-------");
                logger1.info("-------------保存源数据库配置-------");
                ComponentTableInput input = ComponentTableInput.builder()
                        .fkDataSourceId(dataSourceId)
                        .tableName(tableName)
                        .columns(columns)
                        .columnsType(columnsType)
                        .build();
//                int inputId = inputService.saveInput(dataSourceId, columns, columnsType, tableName);
                inputService.save(input);
                SysCommonTask commonTask = SysCommonTask.builder()
                        .id(taskId)
                        .fkInputId(input.getId())
                        .sourceDataType(WholeVariable.SQLSERVER)
                        .build();
                return commonTaskService.updateById(commonTask);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> saveOutput(int taskId, int dataSourceId, String columns, String columnsType, String tableName) {
        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run() throws SQLException {
                logger.info("-------------保存目标数据库配置-------");
                logger1.info("-------------保存目标数据库配置-------");
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
                        .targetDataType(WholeVariable.SQLSERVER)
                        .build();
                return commonTaskService.updateById(commonTask);
            }
        }.execute();
    }

}
