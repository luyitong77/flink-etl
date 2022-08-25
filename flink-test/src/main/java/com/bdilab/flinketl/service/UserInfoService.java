package com.bdilab.flinketl.service;

import com.bdilab.flinketl.entity.UserInfo;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
public interface UserInfoService extends IService<UserInfo> {

    boolean saveUserInfo(String username, String password) throws Exception;
}
