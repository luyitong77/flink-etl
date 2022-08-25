package com.bdilab.flinketl.controller;


import com.bdilab.flinketl.service.ComponentSplitColumnService;
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
 * @since 2021-07-22
 */
@RestController
@RequestMapping("/flinketl/componentSplitColumn")
public class ComponentSplitColumnController {

    @Autowired
    ComponentSplitColumnService splitColumnService;

    @ApiOperation(value = "添加数据分割组件", httpMethod = "POST")
    @ApiResponses({
            @ApiResponse(code = 0, message = "添加成功"),
            @ApiResponse(code = 201, message = "参数值不正确"),
            @ApiResponse(code = 500, message = "系统异常")
    })
    @RequestMapping("/add")
    private ResponseEntity add(
            @ApiParam(name = "taskId", value = "任务id", type = "int")
            @RequestParam(name = "taskId") int taskId,
            @ApiParam(name = "delimiter", value = "分隔符", type = "string")
            @RequestParam(name = "delimiter") String delimiter,
            @ApiParam(name = "splitColumnName", value = "要分割的输入源的列名", type = "string")
            @RequestParam(name = "splitColumnName") String splitColumnName,
            @ApiParam(name = "saveColumnName", value = "分割后保存字段时输出源对应的列名，用空格分割", type = "string")
            @RequestParam(name = "saveColumnName") String saveColumnName,
            @ApiParam(name = "saveColumnType", value = "分割后保存字段时输出源对应列的属性，用空格分割", type = "string")
            @RequestParam(name = "saveColumnType") String saveColumnType,
            @ApiParam(name = "parallelism", value = "并行度", type = "int")
            @RequestParam(name = "parallelism") int parallelism
    ) {
        return ResponseEntity.ok(splitColumnService.add(taskId, delimiter, splitColumnName, saveColumnName, saveColumnType, parallelism));
    }

}

