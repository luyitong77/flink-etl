package com.bdilab.flinketl.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.bdilab.flinketl.entity.UserInfo;
import org.apache.ibatis.annotations.Param;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
public interface UserInfoMapper extends BaseMapper<UserInfo> {

    /**
     * 通过username查找用户信息
     * @param username
     * @return
     */
    UserInfo findUserInfoByUsername(@Param("username") String username);

}
