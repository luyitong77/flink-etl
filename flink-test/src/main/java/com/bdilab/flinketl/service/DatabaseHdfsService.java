package com.bdilab.flinketl.service;

import com.bdilab.flinketl.entity.DatabaseHdfs;
import com.baomidou.mybatisplus.extension.service.IService;
import com.bdilab.flinketl.utils.GlobalResultUtil;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.List;
import java.util.Map;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author hcyong
 * @since 2021-09-08
 */
public interface DatabaseHdfsService extends IService<DatabaseHdfs> {

    /**
     * 测试hdfs连接
     * @param hdfsIp
     * @param hdfsPort
     * @return
     */
    GlobalResultUtil<String> testConnection(String hdfsIp, int hdfsPort);

    /**
     * 保存hdfs配置信息
     * @param configName
     * @param description
     * @param hdfsIp
     * @param hdfsPort
     * @param userId
     * @return
     */
    GlobalResultUtil<Boolean> saveConfig(String configName, String description, String hdfsIp, int hdfsPort, int userId);

    /**
     * 删除hdfs配置
     * @param databaseId
     * @param userDatabaseConfigId
     * @return
     */
    GlobalResultUtil<Boolean> deleteConnection(int databaseId, int userDatabaseConfigId);

    /**
     * 获取指定目录下的所有文件名
     * @param id
     * @param path
     * @return
     */
    GlobalResultUtil<List<ImmutablePair<String, Boolean>>> getAllFiles(int id, String path);

    /**
     * 获取指定文件的第一行数据
     * @param id
     * @param path
     * @return
     */
    GlobalResultUtil<String> getFirstLine(int id, String path);

    /**
     * 添加hdfs的输入组件配置
     * @param taskId
     * @param dataSourceId
     * @param filePath
     * @param columns
     * @param delimiter
     * @return
     */
    GlobalResultUtil<Boolean> addInput(int taskId, int dataSourceId, String filePath, String columns, String delimiter);

}
