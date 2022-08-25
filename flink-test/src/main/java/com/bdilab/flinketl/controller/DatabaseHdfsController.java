package com.bdilab.flinketl.controller;


import com.bdilab.flinketl.service.DatabaseHdfsService;
import com.bdilab.flinketl.utils.common.entity.ResultData;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author hcyong
 * @since 2021-09-08
 */
@RestController
@RequestMapping("/flinketl/hdfs")
public class DatabaseHdfsController {

    @Resource
    DatabaseHdfsService hdfsService;

    @ApiOperation(value = "测试连接", httpMethod = "GET")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/testConnection")
    public ResponseEntity testCon(
            @ApiParam(name = "hdfsIp", value = "hdfs的主机号", required = true, type = "String")
            @RequestParam(name = "hdfsIp") String hdfsIp,
            @ApiParam(name = "hdfsPort", value = "hdfs的端口号", required = true, type = "int")
            @RequestParam(name = "hdfsPort") int hdfsPort
    ) {
        return ResponseEntity.ok(hdfsService.testConnection(hdfsIp, hdfsPort));
    }

    @ApiOperation(value = "保存连接", httpMethod = "POST", notes = "保存hdfs连接")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @PostMapping("/saveConnection")
    public ResponseEntity saveCon(
            @ApiParam(name = "configName", value = "连接配置名", required = true, type = "String")
            @RequestParam(name = "configName") String configName,
            @ApiParam(name = "description", value = "连接描述", required = false, type = "String")
            @RequestParam(name = "description") String description,
            @ApiParam(name = "hdfsIp", value = "hdfs master的ip地址", required = true, type = "String")
            @RequestParam(name = "hdfsIp") String hdfsIp,
            @ApiParam(name = "hdfsPort", value = "hdfs的端口号", required = true, type = "int")
            @RequestParam(name = "hdfsPort") int hdfsPort,
            @ApiParam(name = "userId", value = "用户id", required = true, type = "int")
            @RequestParam(name = "userId") int userId
    ){
        return ResponseEntity.ok(hdfsService.saveConfig(configName, description, hdfsIp, hdfsPort, userId));
    }

    /**
     * 删除连接配置
     * @param databaseId
     * @param userDatabaseConfigId
     * @return
     */
    @ApiOperation(value = "删除hdfs的配置信息", httpMethod = "DELETE")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @DeleteMapping("/deleteConnection")
    public ResponseEntity deleteCon(
            @ApiParam(name = "databaseId", value = "数据库配置Id", type = "int")
            @RequestParam(value = "databaseId") int databaseId,
            @ApiParam(name = "userDatabaseConfigId", value = "信息表id", type = "int")
            @RequestParam(value = "userDatabaseConfigId") int userDatabaseConfigId

    ){
        return ResponseEntity.ok(hdfsService.deleteConnection(databaseId, userDatabaseConfigId));
    }

    @ApiOperation(value = "获取根目录下的所有文件和目录", httpMethod = "GET", notes = "返回值格式 文件或目录名：是否是文件")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/getDefaultFiles")
    public ResponseEntity getDefaultFiles(
            @ApiParam(name = "id", value = "hdfs数据库的id", required = true, type = "int")
            @RequestParam(name = "id") int id
    ) {
        return ResponseEntity.ok(hdfsService.getAllFiles(id, "/"));
    }

    @ApiOperation(value = "获取当前路径下的所有的文件和目录名", httpMethod = "GET", notes = "返回值格式 文件或目录名：是否是文件")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/getAllFiles")
    public ResponseEntity getAllFiles(
            @ApiParam(name = "id", value = "hdfs数据库的id", required = true, type = "int")
            @RequestParam(name = "id") int id,
            @ApiParam(name = "path", value = "hdfs文件夹路径", required = true, type = "String")
            @RequestParam(name = "path") String path) {
        return ResponseEntity.ok(hdfsService.getAllFiles(id, path));
    }

    @ApiOperation(value = "获取特定文件的第一行数据", httpMethod = "GET")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/getFirstLine")
    public ResponseEntity getFirstLine(
            @ApiParam(name = "id", value = "hdfs数据库的id", required = true, type = "int")
            @RequestParam(name = "id") int id,
            @ApiParam(name = "path", value = "hdfs文件路径", required = true, type = "String")
            @RequestParam(name = "path") String path) {
        return ResponseEntity.ok(hdfsService.getFirstLine(id, path));
    }

    @ApiOperation(value = "添加hdfs输入的配置", httpMethod = "GET")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/addInput")
    public ResponseEntity addInAndOutEle(
            @ApiParam(name = "taskId", value = "任务的id", required = true, type = "int")
            @RequestParam(name = "taskId") int taskId,
            @ApiParam(name = "dataSourceId", value = "数据源id", required = true, type = "int")
            @RequestParam(value = "dataSourceId") int dataSourceId,
            @ApiParam(name = "filePath", value = "hdfs文件输入的路径", required = true, type = "String")
            @RequestParam(value = "filePath", required = false) String filePath,
            @ApiParam(name = "columns", value = "文件字段的名称，用空格分开", required = true, type = "String")
            @RequestParam(value = "columns", required = false) String columns,
            @ApiParam(name = "delimiter", value = "文件的分隔符", required = true, type = "String")
            @RequestParam(value = "delimiter", required = false) String delimiter
    ) {

        return ResponseEntity.ok(hdfsService.addInput(taskId, dataSourceId, filePath, columns, delimiter));
    }
}

