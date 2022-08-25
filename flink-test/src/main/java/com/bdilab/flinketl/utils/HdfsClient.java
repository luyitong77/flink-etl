package com.bdilab.flinketl.utils;

import com.bdilab.flinketl.entity.DatabaseHdfs;
import com.bdilab.flinketl.mapper.DatabaseHdfsMapper;
import com.bdilab.flinketl.utils.exception.InfoNotInDatabaseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hcyong
 * @date 2021/9/8
 */
@Slf4j
@Component
public class HdfsClient {

    @Resource
    DatabaseHdfsMapper hdfsMapper;

    public String testConnection(String hdfsIp, int hdfsPort) throws Exception {
        String urlPath = getURLPath(hdfsIp, hdfsPort);
        FileSystem hdfs = getConnection(urlPath);
        try {
            getAllFiles(hdfs, "/");

        } catch (Exception e) {
            throw new IllegalArgumentException("连接失败");
        }
        return "success";
    }

    private String getURLPath(String hdfsIp, int hdfsPort) {
        return "hdfs://" + hdfsIp + ":" + hdfsPort;
    }

    private FileSystem getConnection(String urlPath) throws IOException, InterruptedException {
        FileSystem hdfs = null;
        try {
            Configuration configuration = new Configuration();
            URI uri = new URI(urlPath);
            hdfs = FileSystem.get(uri, configuration, "root");
        } catch (URISyntaxException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("uri 参数错误");
        }
        return hdfs;
    }

    public List<ImmutablePair<String, Boolean>> getAllFiles(int id, String path) throws Exception{
        DatabaseHdfs databaseHdfs = hdfsMapper.selectById(id);
        if (databaseHdfs == null){
            throw new InfoNotInDatabaseException("数据库中不存在该信息");
        }
        FileSystem hdfs = getConnection(getURLPath(databaseHdfs.getHdfsIp(), databaseHdfs.getHdfsPort()));
        return getAllFiles(hdfs, path);
    }

    private List<ImmutablePair<String, Boolean>> getAllFiles(FileSystem hdfs, String path) {
        List<ImmutablePair<String, Boolean>> files = new ArrayList<>();
        FileStatus[] fileStatuses = null;
        try {
            fileStatuses = hdfs.listStatus(new Path(path));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            files.add(new ImmutablePair<String, Boolean>("path doesn't exist", false));
            return files;
        } catch (Exception e) {
            e.printStackTrace();
            files.add(new ImmutablePair<String, Boolean>(e.toString(), false));
            return files;
        }

        for (int i = 0; i < fileStatuses.length; i++) {
            String[] fileNames = fileStatuses[i].getPath().toString().split("/");
            Boolean isFile = fileStatuses[i].isFile();
            files.add(new ImmutablePair<String, Boolean>(fileNames[fileNames.length - 1], isFile));
        }

        return files;
    }

    public String getFirstLine(int id, String path) throws Exception{
        DatabaseHdfs databaseHdfs = hdfsMapper.selectById(id);
        if (databaseHdfs == null){
            throw new InfoNotInDatabaseException("数据库中不存在该信息");
        }
        FileSystem hdfs = getConnection(getURLPath(databaseHdfs.getHdfsIp(),databaseHdfs.getHdfsPort()));

        return getFirstLine(hdfs, path);
    }

    private String getFirstLine(FileSystem hdfs, String path) {
        String result;
        try {
            FSDataInputStream dataInputStream = hdfs.open(new Path(path));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(dataInputStream));
            result = new String(bufferedReader.readLine().getBytes(),"utf-8");
        } catch (IOException e) {
            e.printStackTrace();
            result = "read fail";
        }
        return result;
    }

    public boolean createDirectory(int hdfsDataSourceId, String path) {
        String rawFilePath = "hdfs://%s:%d%s";
        DatabaseHdfs databaseHdfs = hdfsMapper.selectById(hdfsDataSourceId);
        String hdfsIp = databaseHdfs.getHdfsIp();
        int hdfsPort = databaseHdfs.getHdfsPort();
        // 要保存的文件夹路径/file/csvOutput
        String outputHdfsPath = String.format(rawFilePath, hdfsIp, hdfsPort, path);

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(outputHdfsPath), conf, "root");
            Path destinationPathOutputPath = new Path(path);
            if (!fs.exists(destinationPathOutputPath)) {
                System.out.println(path + " 不存在");
                fs.mkdirs(destinationPathOutputPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("-------------创建hdfs目录失败-------------");
            return false;
        }
        return true;
    }
}
