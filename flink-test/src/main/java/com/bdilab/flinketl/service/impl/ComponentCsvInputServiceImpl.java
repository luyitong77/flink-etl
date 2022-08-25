package com.bdilab.flinketl.service.impl;

import com.bdilab.flinketl.entity.ComponentCsvInput;
import com.bdilab.flinketl.entity.DatabaseCsv;
import com.bdilab.flinketl.entity.DatabaseHdfs;
import com.bdilab.flinketl.mapper.ComponentCsvInputMapper;
import com.bdilab.flinketl.mapper.DatabaseCsvMapper;
import com.bdilab.flinketl.service.ComponentCsvInputService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bdilab.flinketl.service.DatabaseHdfsService;
import com.bdilab.flinketl.utils.HdfsClient;
import com.bdilab.flinketl.utils.exception.InfoNotInDatabaseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Arrays;
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
@Service
public class ComponentCsvInputServiceImpl extends ServiceImpl<ComponentCsvInputMapper, ComponentCsvInput> implements ComponentCsvInputService {

    private final static String FILE_SEPARATOR = System.getProperty("file.separator");

    @Value("${input.csv-path}")
    private String inputPath;

    @Value("${output.csv-path}")
    private String outputPath;

    @Resource
    DatabaseCsvMapper csvMapper;
    @Resource
    DatabaseHdfsService hdfsService;
    @Resource
    HdfsClient hdfsClient;

    @Override
    public ImmutablePair<Integer, String> uploadFileAndSave(MultipartFile file, int hdfsDataSourceId) {
        if (file == null){
            return null;
        }
        System.out.println("file size: "+file.getSize());

        String userFileName = file.getOriginalFilename();
        String extension = FilenameUtils.getExtension(userFileName);
        if (!"csv".equals(extension)){
            throw new IllegalArgumentException("上传文件错误");
        }

        String fileName = UUID.randomUUID() + "_" + userFileName;
        boolean uploadFileSuccess = uploadFileToHdfs(file, fileName, hdfsDataSourceId);
        if (uploadFileSuccess) {
            DatabaseCsv databaseCsv = DatabaseCsv.builder()
                    .fileName(fileName)
                    .isExist(true)
                    .userFileName(userFileName)
                    .fkHdfsDataSourceId(hdfsDataSourceId)
                    .build();

            csvMapper.insert(databaseCsv);

            int databaseId = databaseCsv.getId();

            return new ImmutablePair<>(databaseId, userFileName);
        } else {
            return null;
        }
    }

    @Override
    public String uploadFileAndEdit(MultipartFile file, int databaseId) {
        if (file == null){
            return null;
        }
        System.out.println("file size: "+file.getSize());
        String userFileName = file.getOriginalFilename();
        String extension = FilenameUtils.getExtension(userFileName);
        if (!"csv".equals(extension)){
            throw new IllegalArgumentException("上传文件错误");
        }

        DatabaseCsv databaseCsv = csvMapper.selectById(databaseId);
        if (databaseCsv == null){
            throw new InfoNotInDatabaseException("数据库中不存在该信息");
        }

        String fileName = databaseCsv.getFileName();
        int hdfsDataSourceId = databaseCsv.getFkHdfsDataSourceId();
        boolean uploadFileSuccess = uploadFileToHdfs(file, fileName, hdfsDataSourceId);
        if (uploadFileSuccess) {
            databaseCsv.setUserFileName(userFileName);
            csvMapper.updateById(databaseCsv);
            return userFileName;
        } else {
            return null;
        }
    }

    @Override
    public List<String> getAllFields(int databaseId, String delimiter) {
        String encoding = Charset.defaultCharset().name();

        DatabaseCsv databaseCsv = csvMapper.selectById(databaseId);
        if (databaseCsv.getFileName() == null){
            throw new InfoNotInDatabaseException("数据库中不存在该信息");
        }

        String fileName = databaseCsv.getFileName();
        int hdfsDataSourceId = databaseCsv.getFkHdfsDataSourceId();

        String inputHdfsPath = getInputHdfsPath(hdfsDataSourceId);
        String filePath = getInputFilePath(inputHdfsPath, fileName);
        FSDataInputStream inputStream;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(inputHdfsPath), conf, "root");
            inputStream = fs.open(new Path(filePath));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        try {
            InputStreamReader reader = new InputStreamReader(inputStream, encoding);
            BufferedReader bufferedReader = new BufferedReader(reader);
            String line = bufferedReader.readLine();
            if (line == null){
                throw new IllegalArgumentException("文件为空");
            }
            String[] fields = line.split(delimiter);

            return Arrays.asList(fields);

        }catch (FileNotFoundException e){
            e.printStackTrace();
            throw new IllegalArgumentException("文件不存在");
        }catch (UnsupportedEncodingException e){
            e.printStackTrace();
            throw new IllegalArgumentException("不支持该编码");
        }catch (IOException e){
            e.printStackTrace();
            throw new IllegalArgumentException("文件读取失败");
        }
    }

    // 上传文件到hdfs
    private boolean uploadFileToHdfs(MultipartFile file, String fileName, int hdfsDataSourceId) {
        String inputHdfsPath = getInputHdfsPath(hdfsDataSourceId);
        String filePath = getInputFilePath(inputHdfsPath, fileName);

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(inputHdfsPath), conf, "root");
            Path destinationPath = new Path(inputPath);
            if (!fs.exists(destinationPath)) {
                System.out.println(inputPath + " 不存在");
                fs.mkdirs(destinationPath);
            }

            Path destinationPathOutputPath = new Path(outputPath);
            if (!fs.exists(destinationPathOutputPath)) {
                System.out.println(outputPath + " 不存在");
                fs.mkdirs(destinationPathOutputPath);
            }

            FSDataOutputStream outputStream = fs.create(new Path(filePath));
            IOUtils.copyBytes(file.getInputStream(), outputStream, conf, true);

            System.out.println("hdfs文件保存目录：" + fileName);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("-----------文件上传失败----------");
            return false;
        }
        return true;
    }

    @Override
    public String getInputHdfsPath(int hdfsDataSourceId) {
        String rawFilePath = "hdfs://%s:%d%s";
        DatabaseHdfs databaseHdfs = hdfsService.getById(hdfsDataSourceId);
        String hdfsIp = databaseHdfs.getHdfsIp();
        int hdfsPort = databaseHdfs.getHdfsPort();
        // 要保存的文件夹路径/file/csvInput
        String inputHdfsPath = String.format(rawFilePath, hdfsIp, hdfsPort, inputPath);
        return inputHdfsPath;
    }

    @Override
    public String getInputFilePath(String inputHdfsPath, String fileName) {
        String filePath = inputHdfsPath + "/" + fileName;
        return filePath;
    }
}
