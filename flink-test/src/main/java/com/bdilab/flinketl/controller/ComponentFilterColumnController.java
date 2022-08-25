package com.bdilab.flinketl.controller;


import com.bdilab.flinketl.service.ComponentFilterColumnService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
@RestController
@RequestMapping("/flinketl/componentFilterColumn")
public class ComponentFilterColumnController {

    @Autowired
    ComponentFilterColumnService filterColumnService;

    @ApiOperation(value = "添加数据过滤组件", httpMethod = "POST")
    @ApiResponses({
            @ApiResponse(code = 0, message = "添加成功"),
            @ApiResponse(code = 201, message = "参数值不正确"),
            @ApiResponse(code = 500, message = "系统异常")
    })
    @RequestMapping("/add")
    private ResponseEntity add(
            @ApiParam(name = "taskId", value = "任务id", type = "int")
            @RequestParam(name = "taskId") int taskId,
            @ApiParam(name = "column", value = "列名", type = "string")
            @RequestParam(name = "column") String column,
            @ApiParam(name = "type", value = "列名字段属性", type = "string")
            @RequestParam(name = "type") String type,
            @ApiParam(name = "function", value = "过滤函数", type = "string")
            @RequestParam(name = "function") String function,
            @ApiParam(name = "condition", value = "过滤条件", type = "string")
            @RequestParam(name = "condition") String condition,
            @ApiParam(name = "parallelism", value = "并行度", type = "int")
            @RequestParam(name = "parallelism") int parallelism
    ) {
        return ResponseEntity.ok(filterColumnService.add(taskId, column, type, function, condition, parallelism));
    }

}

