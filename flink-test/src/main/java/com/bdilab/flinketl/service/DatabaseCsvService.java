package com.bdilab.flinketl.service;

import com.bdilab.flinketl.entity.DatabaseCsv;
import com.baomidou.mybatisplus.extension.service.IService;
import com.bdilab.flinketl.utils.GlobalResultUtil;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author hcyong
 * @since 2021-08-30
 */
public interface DatabaseCsvService extends IService<DatabaseCsv> {

    /**
     * 上传要读取的csv文件
     * @param configName
     * @param description
     * @param file
     * @param userId
     * @param hdfsDataSourceId
     * @return
     */
    GlobalResultUtil<String> uploadFileAndSave(String configName, String description, MultipartFile file, int hdfsDataSourceId, int userId);

    /**
     * 编辑已上传的csv文件
     * @param configName
     * @param description
     * @param file
     * @param databaseId
     * @param configId
     * @param userId
     * @return
     */
    GlobalResultUtil<String> uploadFileAndEdit(String configName, String description, MultipartFile file, int databaseId, int configId, int userId);

    /**
     * 获取已上传文件的所有字段名
     * @param databaseId
     * @param delimiter
     * @return
     */
    GlobalResultUtil<List<String>> getAllFields(int databaseId, String delimiter);

    /**
     * 添加csv输入组件
     * @param taskId
     * @param dataSourceId
     * @param columns
     * @param delimiter
     * @return
     */
    GlobalResultUtil<Boolean> addInput(int taskId, int dataSourceId, String columns, String columnsType, String delimiter);

    /**
     * 添加csv输出组件
     * @param taskId
     * @param hdfsDataSourceId
     * @param fileName
     * @param columns
     * @param columnsType
     * @param delimiter
     * @return
     */
    GlobalResultUtil<Boolean> addOutput(int taskId, int hdfsDataSourceId, String fileName, String columns, String columnsType, String delimiter);
}
