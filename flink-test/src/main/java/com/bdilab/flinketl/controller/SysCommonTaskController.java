package com.bdilab.flinketl.controller;


import com.bdilab.flinketl.service.SysCommonTaskService;
import com.bdilab.flinketl.service.UserTaskService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
@RestController
@RequestMapping("/flinketl/sysCommonTask")
public class SysCommonTaskController {

    @Autowired
    SysCommonTaskService taskService;

    @ApiOperation(value = "建立Task", httpMethod = "GET", notes = "建立Task")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @GetMapping("/build")
    public ResponseEntity build(
            @ApiParam(name = "taskType", value = "任务类型：0,1,2：普通，定时，触发", type = "int")
            @RequestParam(name = "taskType") int taskType,
            @ApiParam(name = "taskName", value = "任务名称(小于50个字符)", type = "String")
            @RequestParam(name = "taskName") String taskName,
            @ApiParam(name = "userId", value = "用户id", type = "int")
            @RequestParam(name = "userId") int userId,
            @ApiParam(name = "parallelism", value = "并行度", type = "int")
            @RequestParam(name = "parallelism") int parallelism
    ) {
        return ResponseEntity.ok(taskService.build(taskType, taskName, userId, parallelism));
    }

    @ApiOperation(value = "提交Task", httpMethod = "POST", notes = "提交任务")
    @ApiResponses({
            @ApiResponse(code = 0, message = "提交成功"),
            @ApiResponse(code = 201, message = "数据库异常")

    })
    @PostMapping("/commitTask")
    public ResponseEntity commitTask(
            @ApiParam(name = "taskId", value = "任务id", type = "int")
            @RequestParam(name = "taskId") int taskId,
            @ApiParam(name = "stepOrder", value = "任务各个组件步骤之间的顺序 步骤名用空格分隔", type = "String")
            @RequestParam(name = "stepOrder") String stepOrder,
            @ApiParam(name = "parallelism", value = "并行度", type = "int")
            @RequestParam(name = "parallelism") int parallelism,
            @ApiParam(name = "userId", value = "用户id", type = "int")
            @RequestParam(name = "userId") int userId
    ) {
        return ResponseEntity.ok(taskService.commitTask(taskId, stepOrder, parallelism, userId));
    }

    @ApiOperation(value = "执行Task", httpMethod = "POST", notes = "执行Task")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @PostMapping("/run")
    public ResponseEntity run(
            @ApiParam(name = "taskId", value = "任务id", type = "int")
            @RequestParam(name = "taskId") int taskId
    ) {
        return ResponseEntity.ok(taskService.run(taskId));
    }

    @ApiOperation(value = "停止执行Task", httpMethod = "POST", notes = "停止执行Task")
    @ApiResponses({
            @ApiResponse(code = 0, message = "运行成功"),
            @ApiResponse(code = 201, message = "数据库异常"),
            @ApiResponse(code = 204, message = "请求信息不存在"),
            @ApiResponse(code = 407, message = "输入参数错误"),
            @ApiResponse(code = 500, message = "运行错误")
    })
    @PostMapping("/stop")
    public ResponseEntity stop(
            @ApiParam(name = "taskId", value = "任务id", type = "int")
            @RequestParam(name = "taskId") int taskId
    ) {
        return ResponseEntity.ok(taskService.stop(taskId));
    }

    @ApiOperation(value = "获取普通任务列表")
    @PostMapping(value = "/getCommonTaskList")
    public ResponseEntity getCommonTaskList(
            @ApiParam(name = "userId", value = "用户id", type = "int")
            @RequestParam(name = "userId") int userId
    ) {
        return ResponseEntity.ok(taskService.getCommonTaskList(userId));
    }
}

