package com.bdilab.flinketl.service.impl;

import com.bdilab.flinketl.entity.ComponentHdfsInput;
import com.bdilab.flinketl.entity.DatabaseHdfs;
import com.bdilab.flinketl.entity.SysCommonTask;
import com.bdilab.flinketl.entity.UserDatabaseConfig;
import com.bdilab.flinketl.mapper.DatabaseHdfsMapper;
import com.bdilab.flinketl.service.ComponentHdfsInputService;
import com.bdilab.flinketl.service.DatabaseHdfsService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bdilab.flinketl.service.SysCommonTaskService;
import com.bdilab.flinketl.service.UserDatabaseConfigService;
import com.bdilab.flinketl.utils.GlobalResultUtil;
import com.bdilab.flinketl.utils.HdfsClient;
import com.bdilab.flinketl.utils.ResultExecuter;
import com.bdilab.flinketl.utils.WholeVariable;
import com.bdilab.flinketl.utils.exception.InfoNotInDatabaseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author hcyong
 * @since 2021-09-08
 */
@Service
@Slf4j
public class DatabaseHdfsServiceImpl extends ServiceImpl<DatabaseHdfsMapper, DatabaseHdfs> implements DatabaseHdfsService {

    @Resource
    HdfsClient hdfsClient;
    @Resource
    DatabaseHdfsMapper hdfsMapper;
    @Resource
    UserDatabaseConfigService userDatabaseConfigService;
    @Resource
    ComponentHdfsInputService hdfsInputService;
    @Resource
    SysCommonTaskService commonTaskService;

    @Override
    public GlobalResultUtil<String> testConnection(String hdfsIp, int hdfsPort) {
        return new ResultExecuter<String>(){
            @Override
            public String run() throws Exception{
                log.info("执行hdfs连接操作。");

                String result = hdfsClient.testConnection(hdfsIp, hdfsPort);
                log.info("hdfs连接状态："+result);
                return result;
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> saveConfig(String configName, String description, String hdfsIp, int hdfsPort, int userId) {
        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run(){

                log.info("执行hdfs保存连接操作。");
                DatabaseHdfs databaseHdfs = DatabaseHdfs.builder()
                        .hdfsIp(hdfsIp)
                        .hdfsPort(hdfsPort)
                        .build();
                hdfsMapper.insert(databaseHdfs);
                int hdfsId = databaseHdfs.getId();

                if (hdfsId < 1){
                    return false;
                }

                UserDatabaseConfig databaseConfig = UserDatabaseConfig.builder()
                        .configName(configName)
                        .fkDatabaseId(hdfsId)
                        .databaseType(WholeVariable.HDFS_ID)
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
        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run(){
                log.info("删除hdfs配置---------------");

                Boolean deleteHdfsSuccess = hdfsMapper.deleteById(databaseId) == 1;

                //删除hdfs的连接信息
                Boolean deleteUserConfigSuccess = userDatabaseConfigService.deleteUserDatabaseConfigById(userDatabaseConfigId);
                log.info("--------------成功---------------");
                return deleteHdfsSuccess && deleteUserConfigSuccess;
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<List<ImmutablePair<String, Boolean>>> getAllFiles(int id, String path) {
        return new ResultExecuter<List<ImmutablePair<String,Boolean>>>(){
            @Override
            public List<ImmutablePair<String,Boolean>> run() throws Exception{
                log.info("获取指定目录下的所有文件名操作");
                List<ImmutablePair<String, Boolean>> result = new ArrayList<>();
                String oldPath = "";
                if (!"/".equals(path)) {
                    oldPath = path + "/";
                }else {
                    oldPath = path;
                }
                for (ImmutablePair<String, Boolean> pair : hdfsClient.getAllFiles(id, path)) {
                    result.add(new ImmutablePair<String, Boolean>(oldPath + pair.getKey(), pair.getValue()));
                }
                log.info(path+"路径下文件个数为：" + result.size());
                return result;
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<String> getFirstLine(int id, String path) {
        return new ResultExecuter<String>(){
            @Override
            public String run() throws Exception{
                log.info("执行获取指定文件的第一行数据操作");
                String result = hdfsClient.getFirstLine(id, path);
                log.info("Id为："+id+" hdfs文件系统路径为："+path+" 的文件的第一行数据为："+result);
                return result;
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> addInput(int taskId, int dataSourceId, String filePath, String columns, String delimiter) {
        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run(){
                log.info("-----------执行添加hdfs的输入组件配置操作-----------");
                ComponentHdfsInput hdfsInput = ComponentHdfsInput.builder()
                        .fkDataSourceId(dataSourceId)
                        .filePath(filePath)
                        .columns(columns)
                        .delimiter(delimiter)
                        .build();
                hdfsInputService.save(hdfsInput);
                SysCommonTask sysCommonTask = SysCommonTask.builder()
                        .id(taskId)
                        .fkInputId(hdfsInput.getId())
                        .sourceDataType(WholeVariable.HDFS)
                        .build();
                return commonTaskService.updateById(sysCommonTask);
            }
        }.execute();
    }

}
