package com.bdilab.flinketl.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bdilab.flinketl.entity.*;
import com.bdilab.flinketl.mapper.DatabaseOracleMapper;
import com.bdilab.flinketl.service.*;
import com.bdilab.flinketl.utils.*;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.sql.SQLException;
import java.util.List;

/**
 * <p>
 * 数据库配置表 服务实现类
 * </p>
 *
 * @author ljw
 * @since 2021-07-28
 */
@Service
public class DatabaseOracleServiceImpl extends ServiceImpl<DatabaseOracleMapper, DatabaseOracle> implements DatabaseOracleService {

    @Resource
    OracleClient oracleClient;
    @Resource
    DatabaseOracleMapper databaseOracleMapper;
    @Resource
    UserDatabaseConfigService userDatabaseConfigService;
    @Resource
    ComponentTableInputService inputService;
    @Resource
    ComponentTableUpsertService upsertService;
    @Resource
    SysCommonTaskService commonTaskService;

    Logger logger = Logger.getLogger(DatabaseMysqlServiceImpl.class);

    org.slf4j.Logger logger1 = LoggerFactory.getLogger(DatabaseMysqlServiceImpl.class);

    /**
     * 测试数据库连接
     * @return
     */
    @Override
    public GlobalResultUtil<String> testConnection(String hostname, int port, String databaseName, String username, String password, int isServiceName){
        return new ResultExecuter<String>(){
            @Override
            public String run() throws Exception {
                logger.info("数据库配置" +"并测试连接Oracle---------------");
                logger1.info("数据库配置" +"并测试连接---------------");
                DatabaseOracle databaseOracle = DatabaseOracle.builder()
                        .hostname(hostname)
                        .port(port)
                        .databaseName(databaseName)
                        .username(username)
                        .password(password)
                        .isServiceName(isServiceName)
                        .build();
                logger.info("--------------成功---------------");
                return oracleClient.testConnection(databaseOracle);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> saveDatabaseConfig(String configName, String description, String databaseName, String hostName, int port, String userName, String password, int isServiceName, int userId) {
        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run(){
                logger.info("数据库配置 : 保存Oracle连接---------------");
                logger1.info("数据库配置 : 保存连接---------------");
                DatabaseOracle databaseOracle = DatabaseOracle.builder()
                        .hostname(hostName)
                        .port(port)
                        .databaseName(databaseName)
                        .username(userName)
                        .password(password)
                        .isServiceName(isServiceName)
                        .build();
                int databaseId = databaseOracleMapper.insert(databaseOracle);

                if (databaseId < 1){
                    return false;
                }

                UserDatabaseConfig databaseConfig = UserDatabaseConfig.builder()
                        .configName(configName)
                        .fkDatabaseId(databaseId)
                        .databaseType(WholeVariable.ORACLE_ID)
                        .description(description)
                        .fkUserId(userId)
                        .build();
                userDatabaseConfigService.save(databaseConfig);
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
                logger.info("删除Oracle配置---------------");
                logger1.info("删除Oracle连接配置---------------");

                boolean isOracleDelete = databaseOracleMapper.deleteById(databaseId) > 0;

                //删除Oracle的连接信息
                boolean isDatabaseConfigDelete = userDatabaseConfigService.deleteUserDatabaseConfigById(userDatabaseConfigId);
                logger.info("--------------成功---------------");
                return isOracleDelete && isDatabaseConfigDelete;
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<List<String>> getALlSchemas(String hostname, int port, String databaseName, String username, String password, int isServiceName) {
        return new ResultExecuter<List<String>>() {
            @Override
            public List<String> run() throws SQLException {
                logger.info("-------------获取Oracle数据库配置" + hostname + "下的所有模式-------");
                logger1.info("-------------获取Oracle数据库配置" + hostname + "下的所有模式-------");
                DatabaseOracle databaseOracle = DatabaseOracle.builder()
                        .hostname(hostname)
                        .port(port)
                        .databaseName(databaseName)
                        .username(username)
                        .password(password)
                        .isServiceName(isServiceName)
                        .build();
                return oracleClient.getALlSchemas(databaseOracle);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<PageBean<String>> getAllTables(long id, int currentPage, int pageSize) {
        return new ResultExecuter<PageBean<String>>(){
            @Override
            public PageBean<String> run() throws SQLException{
                logger.info("-------------获取Oracle数据库配置" + id + "下的数据库的所有表-------");
                logger1.info("-------------获取Oracle数据库配置" + id + "下的数据库的所有表-------");

                List<String> results =  oracleClient.getAllTables(id);
                return SolveReturnResults.returnPageFromResults(results, currentPage, pageSize);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<List<String>> getAllColumns(long id, String tableName) {
        return new ResultExecuter<List<String>>(){
            @Override
            public List<String> run() throws SQLException {
                logger.info("-------------获取Oracle数据库配置" + id + "下的数据库的表" +tableName+ "的所有列名-------");
                logger1.info("-------------获取Oracle数据库配置" + id + "下的数据库的表" + tableName+ "的所有列名-------");
                return oracleClient.getAllColumns(id, tableName);
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
            public Boolean run() throws SQLException {
                logger.info("-------------保存目标数据库配置-------");
                logger1.info("-------------保存目标数据库配置-------");

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
    public Boolean checkSql(String sql, Long databaseId) {
        return oracleClient.checkSql(sql, databaseId);
    }
}
