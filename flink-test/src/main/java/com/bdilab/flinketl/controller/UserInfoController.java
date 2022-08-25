package com.bdilab.flinketl.controller;


import com.bdilab.flinketl.service.UserInfoService;
import com.bdilab.flinketl.utils.GlobalResultUtil;
import io.swagger.annotations.*;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
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
@RequestMapping("/flinketl/userInfo")
@Api(value = "用户信息获取接口类")
public class UserInfoController {
    @Autowired
    private UserInfoService userInfoService;

    private Logger logger = Logger.getLogger(UserInfoController.class);

    private org.slf4j.Logger logger1 = LoggerFactory.getLogger(UserInfoController.class);

    /**
     * 增加用户;
     *
     * @param username
     * @param password
     */
    @ApiOperation(value = "添加用户", httpMethod = "GET", notes = "")
    @ApiResponses({
            @ApiResponse(code = 0, message = "添加成功"),
            @ApiResponse(code = 201, message = "添加失败"),
            @ApiResponse(code = 202, message = "此角色已存在")
    })
    @GetMapping("/saveUserInfo")
    public ResponseEntity saveUserInfo(
            @ApiParam(name = "username", value = "username", type = "String")
            @RequestParam(value = "username") String username,
            @ApiParam(name = "password", value = "password", type = "String")
            @RequestParam(value = "password") String password,
            @ApiParam(name = "role", value = "角色id，使用空格隔开(0 1 2 3 )", type = "String")
            @RequestParam(value = "role") String role

    ) {
        logger.info("------------添加用户------------");
        logger1.info("------------添加用户------------");

        try {
            if (userInfoService.saveUserInfo(username, password)) {
                return ResponseEntity.ok(GlobalResultUtil.ok());
            } else {
                logger.info("此用户已存在");
                return ResponseEntity.ok(GlobalResultUtil.build(202, "此用户已存在", username));
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
            return ResponseEntity.ok(GlobalResultUtil.build(201, "添加失败", username));
        }
    }
}

