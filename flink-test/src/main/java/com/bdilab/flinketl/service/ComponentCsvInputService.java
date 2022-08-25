package com.bdilab.flinketl.service;

import com.bdilab.flinketl.entity.ComponentCsvInput;
import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
public interface ComponentCsvInputService extends IService<ComponentCsvInput> {

    /**
     * 上传要读取的文件
     * @param file
     * @param hdfsDataSourceId
     * @return
     */
    ImmutablePair<Integer, String> uploadFileAndSave(MultipartFile file, int hdfsDataSourceId);

    /**
     * 上传文件并编辑配置
     * @param file
     * @param databaseId
     * @return
     */
    String uploadFileAndEdit(MultipartFile file, int databaseId);

    /**
     * 获取csv文件的所有字段
     * @param databaseId
     * @param delimiter
     * @return
     */
    List<String> getAllFields(int databaseId, String delimiter);

    /**
     * 文件路径/file/csvInput/
     * @param hdfsDataSourceId
     * @return
     */
    String getInputHdfsPath(int hdfsDataSourceId);

    /**
     * 文件路径/file/csvInput/文件名.csv
     * @param inputHdfsPath
     * @param fileName
     * @return
     */
    String getInputFilePath(String inputHdfsPath, String fileName);
}

