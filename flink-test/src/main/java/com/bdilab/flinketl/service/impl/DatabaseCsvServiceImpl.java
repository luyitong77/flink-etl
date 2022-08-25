package com.bdilab.flinketl.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bdilab.flinketl.entity.*;
import com.bdilab.flinketl.mapper.DatabaseCsvMapper;
import com.bdilab.flinketl.service.*;
import com.bdilab.flinketl.utils.GlobalResultUtil;
import com.bdilab.flinketl.utils.ResultExecuter;
import com.bdilab.flinketl.utils.WholeVariable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.util.List;
import java.util.UUID;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author hcyong
 * @since 2021-08-30
 */
@Slf4j
@Service
public class DatabaseCsvServiceImpl extends ServiceImpl<DatabaseCsvMapper, DatabaseCsv> implements DatabaseCsvService {


    @Resource
    UserDatabaseConfigService userDatabaseConfigService;
    @Resource
    ComponentCsvInputService csvInputService;
    @Resource
    ComponentCsvOutputService csvOutputService;
    @Resource
    SysCommonTaskService commonTaskService;

    @Override
    public GlobalResultUtil<String> uploadFileAndSave(String configName, String description, MultipartFile file, int hdfsDataSourceId, int userId) {
        return new ResultExecuter<String>(){
            @Override
            public String run(){
                log.info("上传并保存csv文件");
                ImmutablePair<Integer, String> results = csvInputService.uploadFileAndSave(file, hdfsDataSourceId);
                if (results == null){
                    return null;
                }
                int databaseId = results.getLeft();

                UserDatabaseConfig databaseConfig = UserDatabaseConfig.builder()
                        .configName(configName)
                        .fkDatabaseId(databaseId)
                        .databaseType(WholeVariable.CSV_ID)
                        .description(description)
                        .fkUserId(userId)
                        .build();

                userDatabaseConfigService.save(databaseConfig);

                log.info("--------------成功---------------");
                return results.getRight();
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<String> uploadFileAndEdit(String configName, String description, MultipartFile file, int databaseId, int configId, int userId) {
        return new ResultExecuter<String>(){
            @Override
            public String run(){
                log.info("上传并编辑csv文件");

                String userFileName = csvInputService.uploadFileAndEdit(file, databaseId);

                if (userFileName == null) {
                    return null;
                }
                UserDatabaseConfig databaseConfig = UserDatabaseConfig.builder()
                        .id(configId)
                        .configName(configName)
                        .fkDatabaseId(databaseId)
                        .databaseType(WholeVariable.CSV_ID)
                        .description(description)
                        .fkUserId(userId)
                        .build();
                userDatabaseConfigService.updateById(databaseConfig);

                log.info("--------------成功---------------");
                return userFileName;
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<List<String>> getAllFields(int databaseId, String delimiter) {
        return new ResultExecuter<List<String>>(){
            @Override
            public List<String> run(){
                log.info("-------------获取已上传文件的所有字段名" + databaseId + "--------------");
                return csvInputService.getAllFields(databaseId, delimiter);
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> addInput(int taskId, int dataSourceId, String columns, String columnsType, String delimiter) {
        return new ResultExecuter<Boolean>() {
            @Override
            public Boolean run() throws Exception {
                log.info("---------保存csv输入组件--------");
                ComponentCsvInput csvInput = ComponentCsvInput.builder()
                        .fkDataSourceId(dataSourceId)
                        .columns(columns)
                        .columnsType(columnsType)
                        .delimiter(delimiter)
                        .build();

                csvInputService.save(csvInput);

                SysCommonTask commonTask = SysCommonTask.builder()
                        .id(taskId)
                        .fkInputId(csvInput.getId())
                        .sourceDataType(WholeVariable.CSV)
                        .build();

                boolean saveInputSuccess = commonTaskService.updateById(commonTask);
                log.info("---------保存 " + saveInputSuccess + "------------");
                return saveInputSuccess;
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> addOutput(int taskId, int hdfsDataSourceId, String fileName, String columns, String columnsType, String delimiter) {
        return new ResultExecuter<Boolean>() {
            @Override
            public Boolean run() throws Exception {
                log.info("---------保存csv输出组件--------");

                String outputFileName = UUID.randomUUID() + "_" + fileName + ".csv";
                ComponentCsvOutput csvOutput = ComponentCsvOutput.builder()
                        .fkHdfsDataSourceId(hdfsDataSourceId)
                        .fileName(outputFileName)
                        .columns(columns)
                        .columnsType(columnsType)
                        .delimiter(delimiter)
                        .build();

                csvOutputService.save(csvOutput);

                SysCommonTask commonTask = SysCommonTask.builder()
                        .id(taskId)
                        .fkUpsertId(csvOutput.getId())
                        .sourceDataType(WholeVariable.CSV)
                        .build();

                boolean saveOutputSuccess = commonTaskService.updateById(commonTask);
                log.info("---------保存 " + saveOutputSuccess + "------------");
                return saveOutputSuccess;
            }
        }.execute();
    }
}
