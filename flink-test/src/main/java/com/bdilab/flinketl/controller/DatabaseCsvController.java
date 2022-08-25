package com.bdilab.flinketl.controller;


import com.bdilab.flinketl.service.DatabaseCsvService;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import org.springframework.web.multipart.MultipartFile;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author hcyong
 * @since 2021-08-30
 */
@RestController
@RequestMapping("/flinketl/csv")
@Api(value = "csv接口类")
public class DatabaseCsvController {

    @Autowired
    DatabaseCsvService csvService;

    @ApiOperation(value = "上传要读取的csv文件", httpMethod = "POST")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @PostMapping("/uploadFileAndSave")
    public ResponseEntity uploadFile(
            @ApiParam(name = "configName", value = "连接配置名", required = true, type = "String")
            @RequestParam(name = "configName") String configName,
            @ApiParam(name = "description", value = "连接描述", required = false, type = "String")
            @RequestParam(name = "description") String description,
            @ApiParam(name = "file", value = "上传的文件", type = "file")
            @RequestParam(value = "file") MultipartFile file,
            @ApiParam(name = "hdfsDataSourceId", value = "hdfs数据源id", type = "Integer")
            @RequestParam(value = "hdfsDataSourceId") int hdfsDataSourceId,
            @ApiParam(name = "userId", value = "用户id", type = "Integer")
            @RequestParam(value = "userId") int userId
    ) {
        return ResponseEntity.ok(csvService.uploadFileAndSave(configName,description, file, hdfsDataSourceId, userId));
    }

    @ApiOperation(value = "编辑已上传的csv文件", httpMethod = "PUT")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @PutMapping("/uploadFileAndEdit")
    public ResponseEntity editFile(
            @ApiParam(name = "configName", value = "连接配置名", required = true, type = "String")
            @RequestParam(name = "configName") String configName,
            @ApiParam(name = "description", value = "连接描述", required = false, type = "String")
            @RequestParam(name = "description") String description,
            @ApiParam(name = "file", value = "上传的文件", type = "file")
            @RequestParam(value = "file") MultipartFile file,
            @ApiParam(name = "databaseId", value = "连接配置数据库的id", required = true, type = "int")
            @RequestParam(name = "databaseId") int databaseId,
            @ApiParam(name = "configId", value = "用户数据库配置id", required = true, type = "int")
            @RequestParam(name = "configId") int configId,
            @ApiParam(name = "userId", value = "用户id", required = true, type = "int")
            @RequestParam(name = "userId") int userId
    ) {
        return ResponseEntity.ok(csvService.uploadFileAndEdit(configName, description, file, databaseId, configId, userId));
    }

    @ApiOperation(value = "编辑已上传的csv文件配置,不需要上传文件", httpMethod = "PUT")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @PutMapping("/editConfig")
    public ResponseEntity editConfig(
            @ApiParam(name = "configName", value = "连接配置名", required = true, type = "String")
            @RequestParam(name = "configName") String configName,
            @ApiParam(name = "description", value = "连接描述", required = false, type = "String")
            @RequestParam(name = "description") String description,
            @ApiParam(name = "databaseId", value = "连接配置数据库的id", type = "int")
            @RequestParam(name = "databaseId") int databaseId,
            @ApiParam(name = "configId", value = "用户连接配置表的id", required = true, type = "int")
            @RequestParam(name = "configId") int configId,
            @ApiParam(name = "userId", value = "用户id", required = true, type = "int")
            @RequestParam(name = "userId") int userId
    ) {
        return ResponseEntity.ok(csvService.uploadFileAndEdit(configName, description, null, databaseId, configId, userId));
    }

    @ApiOperation(value = "获取已上传文件的所有字段名", httpMethod = "GET")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/getAllFields")
    public ResponseEntity getAllFields(
            @ApiParam(name = "databaseId", value = "csv文件的数据库Id", type = "int")
            @RequestParam(value = "databaseId") int databaseId,
            @ApiParam(name = "delimiter", value = "csv文件的分隔符", type = "String")
            @RequestParam(value = "delimiter") String delimiter
    ) {
        return ResponseEntity.ok(csvService.getAllFields(databaseId, delimiter));
    }

    @ApiOperation(value = "添加csv输入组件", httpMethod = "GET")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/addInput")
    public ResponseEntity addInput(
            @ApiParam(name = "taskId", value = "任务Id", type = "int")
            @RequestParam(value = "taskId") int taskId,
            @ApiParam(name = "dataSourceId", value = "数据源id", required = true, type = "int")
            @RequestParam(value = "dataSourceId") int dataSourceId,
            @ApiParam(name = "columns", value = "输入的列名", type = "String")
            @RequestParam(value = "columns") String columns,
            @ApiParam(name = "columnsType", value = "输入的列名的属性", required = true, type = "String")
            @RequestParam(value = "columnsType", required = false) String columnsType,
            @ApiParam(name = "delimiter", value = "文件的分隔符", type = "String")
            @RequestParam(value = "delimiter") String delimiter

    ) {
        return ResponseEntity.ok(csvService.addInput(taskId, dataSourceId, columns, columnsType, delimiter));
    }

    @ApiOperation(value = "添加csv输出组件", httpMethod = "GET")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/addOutput")
    public ResponseEntity addOutput(
            @ApiParam(name = "taskId", value = "任务Id", type = "int")
            @RequestParam(value = "taskId") int taskId,
            @ApiParam(name = "hdfsDataSourceId", value = "数据源id", required = true, type = "int")
            @RequestParam(value = "hdfsDataSourceId") int hdfsDataSourceId,
            @ApiParam(name = "fileName", value = "输出文件名", required = true, type = "String")
            @RequestParam(value = "fileName") String fileName,
            @ApiParam(name = "columns", value = "输入的列名", type = "String")
            @RequestParam(value = "columns") String columns,
            @ApiParam(name = "columnsType", value = "输入的列名的属性，参考https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/types/", required = true, type = "String")
            @RequestParam(value = "columnsType", required = false) String columnsType,
            @ApiParam(name = "delimiter", value = "文件的分隔符", type = "String")
            @RequestParam(value = "delimiter") String delimiter

    ) {
        return ResponseEntity.ok(csvService.addOutput(taskId, hdfsDataSourceId, fileName, columns, columnsType, delimiter));
    }

}

