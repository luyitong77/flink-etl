package com.bdilab.flinketl.controller;

import com.bdilab.flinketl.service.DatabaseSqlServerService;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * @author jlm
 * @since 2021-07-25
 */
@RestController
@RequestMapping("/flinketl/sqlServer")
@Api(value="sqlServer数据库接口类")
public class DatabaseSqlServerController {
    @Autowired
    DatabaseSqlServerService databaseSqlServerService;

    /**
     * 测试数据库连接
     * @param hostname
     * @param port
     * @param username
     * @param password
     * @return
     */
    @ApiOperation(value = "测试连接", httpMethod = "GET", notes = "测试sqlServer连接")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/testConnection")
    public ResponseEntity testConnection(
            @ApiParam(name = "hostname", value = "数据库ip", required = true, type = "String")
            @RequestParam(name = "hostname") String hostname,
            @ApiParam(name = "port", value = "端口号", required = true, type = "int")
            @RequestParam(name = "port") int port,
            @ApiParam(name = "username", value = "数据库用户名", required = true, type = "String")
            @RequestParam(name = "username") String username,
            @ApiParam(name = "password", value = "数据库连接密码", required = true, type = "String")
            @RequestParam(name = "password") String password
    ) {

        return ResponseEntity.ok(databaseSqlServerService.testConnection(hostname, port, username, password));
    }

    /**
     * 保存数据库连接
     *
     * @param configName
     * @param description
     * @param hostname
     * @param port
     * @param username
     * @param password
     * @param databaseName
     * @return
     */
    @ApiOperation(value = "保存连接", httpMethod = "POST", notes = "保存sqlServer连接")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @PostMapping("/saveConnection")
    public ResponseEntity saveConnection(
            @ApiParam(name = "configName", value = "连接配置名", required = true, type = "String")
            @RequestParam(name = "configName") String configName,
            @ApiParam(name = "description", value = "连接描述", required = false, type = "String")
            @RequestParam(name = "description") String description,
            @ApiParam(name = "hostname", value = "数据库ip", required = true, type = "String")
            @RequestParam(name = "hostname") String hostname,
            @ApiParam(name = "port", value = "端口号", required = true, type = "int")
            @RequestParam(name = "port") int port,
            @ApiParam(name = "username", value = "数据库用户名", required = true, type = "String")
            @RequestParam(name = "username") String username,
            @ApiParam(name = "password", value = "数据库连接密码", required = true, type = "String")
            @RequestParam(name = "password") String password,
            @ApiParam(name = "databaseName", value = "数据库名", required = true, type = "String")
            @RequestParam(name = "databaseName") String databaseName,
            @ApiParam(name = "userId", value = "用户id", required = true, type = "int")
            @RequestParam(name = "userId") int userId
    ) {
        return ResponseEntity.ok(databaseSqlServerService.saveDatabaseConfig(configName, description, databaseName, hostname, port, username, password, userId));
    }

    /**
     * 删除连接配置
     *
     * @param databaseId
     * @param userDatabaseConfigId
     * @return
     */
    @ApiOperation(value = "删除sqlServer的配置信息", httpMethod = "DELETE")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @DeleteMapping("/deleteConnection")
    public ResponseEntity deleteConnection(
            @ApiParam(name = "databaseId", value = "数据库配置id", type = "int")
            @RequestParam(value = "databaseId") int databaseId,
            @ApiParam(name = "userDatabaseConfigId", value = "信息表id", type = "long")
            @RequestParam(value = "userDatabaseConfigId") int userDatabaseConfigId

    ) {
        return ResponseEntity.ok(databaseSqlServerService.deleteConnection(databaseId, userDatabaseConfigId));
    }

    /**
     * 获取sqlServer的所有数据库
     *
     * @return
     */
    @ApiOperation(value = "获取sqlServer下的所有数据库", httpMethod = "GET", notes = "获取sqlServer下的所有数据库")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/getAllDatabase")
    public ResponseEntity getAllDatabase(
            @ApiParam(name = "hostname", value = "数据库ip", required = true, type = "String")
            @RequestParam(name = "hostname") String hostname,
            @ApiParam(name = "port", value = "端口号", required = true, type = "int")
            @RequestParam(name = "port") int port,
            @ApiParam(name = "username", value = "数据库用户名", required = true, type = "String")
            @RequestParam(name = "username") String username,
            @ApiParam(name = "password", value = "数据库连接密码", required = true, type = "String")
            @RequestParam(name = "password") String password
    ) {
        return ResponseEntity.ok(databaseSqlServerService.getAllDatabases(hostname, port, username, password));
    }

    /**
     * 获取输入数据库下的所有的表
     *
     * @param id
     * @return
     */
    @ApiOperation(value = "获取mysql指定数据库下所有的表", httpMethod = "GET", notes = "获取mysql指定数据库下所有的表")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/getAllTables")
    public ResponseEntity getAllTables(
            @ApiParam(name = "id", value = "数据库的id", required = true, type = "int")
            @RequestParam(value = "id") int id,
            @ApiParam(name = "currentPage", value = "当前页", required = true, type = "int")
            @RequestParam(value = "currentPage") int currentPage,
            @ApiParam(name = "pageSize", value = "页大小", required = true, type = "int")
            @RequestParam(value = "pageSize") int pageSize
    ) {
        return ResponseEntity.ok(databaseSqlServerService.getAllTables(id, currentPage, pageSize));
    }


    /**
     * 获取指定数据库下的指定表下的所有列
     *
     * @param id
     * @param tableName
     * @return
     */
    @ApiOperation(value = "获取sqlServer指定表下所有的列名", httpMethod = "GET", notes = "获取sqlService指定表下所有的列名")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/getAllColumns")
    public ResponseEntity getAllColumns(
            @ApiParam(name = "id", value = "数据库的id", required = true, type = "int")
            @RequestParam(value = "id") int id,
            @ApiParam(name = "table", value = "表名", required = true, type = "String")
            @RequestParam(value = "table") String tableName
    ) {
        return ResponseEntity.ok(databaseSqlServerService.getAllColumns(id, tableName));
    }

    @ApiOperation(value = "添加sqlServer输入组件", httpMethod = "GET", notes = "sqlServer输入组件")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/addInput")
    public ResponseEntity addInputComponent(
            @ApiParam(name = "taskId", value = "任务的id", required = true, type = "int")
            @RequestParam(value = "taskId") int taskId,
            @ApiParam(name = "dataSourceId", value = "数据源id", required = true, type = "int")
            @RequestParam(value = "dataSourceId") int dataSourceId,
            @ApiParam(name = "columns", value = "源数据库表字段，用空格隔开", required = true, type = "String")
            @RequestParam(value = "columns") String columns,
            @ApiParam(name = "columnsType", value = "源数据库表字段属性，用空格隔开", required = true, type = "String")
            @RequestParam(value = "columnsType") String columnsType,
            @ApiParam(name = "tableName", value = "源数据库表名", required = true, type = "String")
            @RequestParam(value = "tableName") String tableName
    ) {
        return ResponseEntity.ok(databaseSqlServerService.saveInput(taskId, dataSourceId, columns, columnsType, tableName));
    }

    @ApiOperation(value = "添加sqlServer输出组件", httpMethod = "GET", notes = "sqlServer输出组件")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/addOutput")
    public ResponseEntity addOutputComponent(
            @ApiParam(name = "taskId", value = "任务的id", required = true, type = "int")
            @RequestParam(value = "taskId") int taskId,
            @ApiParam(name = "dataSourceId", value = "数据源id", required = true, type = "int")
            @RequestParam(value = "dataSourceId") int dataSourceId,
            @ApiParam(name = "columns", value = "目标数据库表字段，用空格隔开", required = true, type = "String")
            @RequestParam(value = "columns") String columns,
            @ApiParam(name = "columnsType", value = "目标数据库表字段属性，用空格隔开", required = true, type = "String")
            @RequestParam(value = "columnsType") String columnsType,
            @ApiParam(name = "tableName", value = "目标数据库表名", required = true, type = "String")
            @RequestParam(value = "tableName") String tableName
    ) {
        return ResponseEntity.ok(databaseSqlServerService.saveOutput(taskId, dataSourceId, columns, columnsType, tableName));
    }
}
