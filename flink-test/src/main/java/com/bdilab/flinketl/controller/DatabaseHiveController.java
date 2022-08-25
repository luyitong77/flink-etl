package com.bdilab.flinketl.controller;

import com.bdilab.flinketl.service.DatabaseHiveService;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;

@RestController
@RequestMapping("/flinketl/hive")
@Api(value = "hive数据库接口类")
public class DatabaseHiveController {
    @Autowired
    DatabaseHiveService hiveService;

    /**
     * 测试数据库连接
     *
     * @param hostname
     * @param port
     * @param username
     * @param password
     * @return
     */
    @ApiOperation(value = "测试连接", httpMethod = "GET", notes = "测试mysql连接")
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

        return ResponseEntity.ok(hiveService.testConnection(hostname, port, username, password));
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
    @ApiOperation(value = "保存连接", httpMethod = "POST", notes = "保存hive连接")
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
            @ApiParam(name = "fkUserId", value = "用户id", required = true, type = "int")
            @RequestParam(name = "fkUserId") int userId
    ) {
        return ResponseEntity.ok(hiveService.saveDatabaseConfig(configName, description, databaseName, hostname, port, username, password, userId));
    }

    /**
     * 删除连接配置
     *
     * @param databaseId
     * @param userDatabaseConfigId
     * @return
     */
    @ApiOperation(value = "删除hive的配置信息", httpMethod = "DELETE")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @DeleteMapping("/deleteConnection")
    public ResponseEntity deleteConnection(
            @ApiParam(name = "fkDatabaseId", value = "数据库配置id", type = "int")
            @RequestParam(value = "fkDatabaseId") int databaseId,
            @ApiParam(name = "userDatabaseConfigId", value = "信息表id", type = "long")
            @RequestParam(value = "userDatabaseConfigId") int userDatabaseConfigId

    ) {
        return ResponseEntity.ok(hiveService.deleteConnection(databaseId, userDatabaseConfigId));
    }

    /**
     * 获取hive的所有数据库
     *
     * @return
     */
    @ApiOperation(value = "获取hive下的所有数据库", httpMethod = "GET", notes = "获取hive下的所有数据库")
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
        return ResponseEntity.ok(hiveService.getAllDatabases(hostname, port, username, password));
    }


    /**
     * 获取输入数据库下的所有的表
     *
     * @param id
     * @return
     */
    @ApiOperation(value = "获取hive指定数据库下所有的表", httpMethod = "GET", notes = "获取hive指定数据库下所有的表")
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
        return ResponseEntity.ok(hiveService.getAllTables(id, currentPage, pageSize));
    }

    /**
     * 获取该数据库中表结构
     *
     * @param id
     * @param tableName
     * @return
     */
    @ApiOperation(value = "获取hive指定表下表结构", httpMethod = "GET", notes = "获取指定表下表结构")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/getTableStructureInfo")
    public ResponseEntity getTableStructureInfo(
            @ApiParam(name = "id", value = "数据库的id", required = true, type = "long")
            @RequestParam(value = "id") long id,
            @ApiParam(name = "table", value = "表名", required = true, type = "String")
            @RequestParam(value = "table") String tableName
    ) throws SQLException {
        return ResponseEntity.ok(hiveService.getTableStructureInfo(id, tableName));
    }


    /**
     * 获取指定数据库下的指定表下的所有列
     *
     * @param id
     * @param tableName
     * @return
     */
    @ApiOperation(value = "获取hive指定表下所有的列名", httpMethod = "GET", notes = "获取hive指定表下所有的列名")
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
        return ResponseEntity.ok(hiveService.getAllColumns(id, tableName));
    }

    /**
     * 获取指定数据库下的指定表下的所有数据
     *
     * @param id
     * @param tableName
     * @return
     */
    @ApiOperation(value = "获取hive指定表下所有的数据", httpMethod = "GET", notes = "获取hive指定表下所有的数据")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/getDataPreview")
    public ResponseEntity getDataPreview(
            @ApiParam(name = "id", value = "数据库的id", required = true, type = "int")
            @RequestParam(value = "id") int id,
            @ApiParam(name = "table", value = "表名", required = true, type = "String")
            @RequestParam(value = "table") String tableName
    ) {
        return ResponseEntity.ok(hiveService.getDataPreview(id, tableName));
    }


    /**
     * 添加hive的输入组件（输入不需要考虑分区）
     * @param taskId
     * @param dataSourceId
     * @param columns
     * @param columnsType
     * @param tableName
     * @param customSql
     * @return
     */
    @ApiOperation(value = "添加hive的输入组件（输入不需要考虑分区）", httpMethod = "GET", notes = "hive的输入组件（输入不考虑分区）")
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
            @ApiParam(name = "columns", value = "源数据库表字段，用空格隔开", required = false, type = "String")
            @RequestParam(value = "columns", required = false) String columns,
            @ApiParam(name = "columnsType", value = "源数据库表字段属性，用空格隔开", required = false, type = "String")
            @RequestParam(value = "columnsType", required = false) String columnsType,
            @ApiParam(name = "tableName", value = "源数据库表名", required = false, type = "String")
            @RequestParam(value = "tableName", required = false) String tableName,
            @ApiParam(name = "customSql", value = "自定义sql查询语句", required = false, type = "String")
            @RequestParam(value = "customSql", required = false) String customSql
    ) {
        return ResponseEntity.ok(hiveService.saveInput(taskId, dataSourceId, columns, columnsType, tableName, customSql));
    }

    @ApiOperation(value = "添加hive输出组件（考虑分区）", httpMethod = "GET", notes = "hive输出组件（考虑分区）")
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
            @RequestParam(value = "tableName") String tableName,
            @ApiParam(name = "isPartitioned", value = "是否采用分区", required = true, type = "int")
            @RequestParam(value = "isPartitioned") int isPartitioned,
            @ApiParam(name = "partitonCol", value = "分区列名，用空格隔开", required = true, type = "String")
            @RequestParam(value = "partitonCol") String partitonCol,
            @ApiParam(name = "partitonColType", value = "分区列名属性，用空格隔开", required = true, type = "String")
            @RequestParam(value = "partitonColType") String partitonColType


    ) {
        return ResponseEntity.ok(hiveService.saveOutput(taskId, dataSourceId, columns, columnsType, tableName,isPartitioned,partitonCol,partitonColType));
    }
}
