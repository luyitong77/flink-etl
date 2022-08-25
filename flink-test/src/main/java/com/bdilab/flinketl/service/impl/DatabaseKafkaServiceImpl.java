package com.bdilab.flinketl.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bdilab.flinketl.entity.DatabaseKafka;
import com.bdilab.flinketl.mapper.DatabaseKafkaMapper;
import com.bdilab.flinketl.service.DatabaseKafkaService;
import com.bdilab.flinketl.utils.GlobalResultUtil;
import com.bdilab.flinketl.utils.PageBean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
//import com.bdilab.flinketl.service.ComponentTableInputService;
//import com.bdilab.flinketl.service.ComponentTableUpsertService;
//import com.bdilab.flinketl.service.SysCommonTaskService;

import javax.annotation.Resource;
import java.util.List;

/**
 * @description:
 * @author: ljw
 * @time: 2021/10/19 20:11
 */
@Slf4j
@Service
public class DatabaseKafkaServiceImpl  extends ServiceImpl<DatabaseKafkaMapper, DatabaseKafka> implements DatabaseKafkaService {

    @Resource
    DatabaseKafkaMapper databaseKafkaMapper;

//    @Resource
//    ComponentTableInputService inputService;
//    @Resource
//    ComponentTableUpsertService upsertService;
//    @Resource
//    SysCommonTaskService commonTaskService;

//    @Override
//    public DatabaseKafka getKafkaInfo(int id) {
//        return databaseKafkaMapper.selectById(id);
//    }

    @Override
    public GlobalResultUtil<String> testConnection(String hostname, int port, String databaseName, String username, String password, int isServiceName) {
        return null;
    }

    @Override
    public GlobalResultUtil<Boolean> saveDatabaseConfig(String configName, String description, String databaseName, String hostname, int port, String username, String password, int isServiceName, int userId) {
        return null;
    }

    @Override
    public GlobalResultUtil<Boolean> deleteConnection(int databaseId, int userDatabaseConfigId) {
        return null;
    }

    @Override
    public GlobalResultUtil<List<String>> getALlSchemas(String hostname, int port, String databaseName, String username, String password, int isServiceName) {
        return null;
    }

    @Override
    public GlobalResultUtil<PageBean<String>> getAllTables(long id, int currentPage, int pageSize) {
        return null;
    }

    @Override
    public GlobalResultUtil<List<String>> getAllColumns(long id, String tableName) {
        return null;
    }

    @Override
    public GlobalResultUtil<Boolean> saveInput(int taskId, int dataSourceId, String columns, String columnsType, String tableName) {
        return null;
    }

    @Override
    public GlobalResultUtil<Boolean> saveOutput(int taskId, int dataSourceId, String columns, String columnsType, String tableName) {
        return null;
    }
}
