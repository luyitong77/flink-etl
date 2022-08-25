package com.bdilab.flinketl.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bdilab.flinketl.entity.UserInfo;
import com.bdilab.flinketl.mapper.UserInfoMapper;
import com.bdilab.flinketl.service.UserInfoService;
import com.bdilab.flinketl.utils.MD5Util;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
@Service
public class UserInfoServiceImpl extends ServiceImpl<UserInfoMapper, UserInfo> implements UserInfoService {

    @Resource
    UserInfoMapper userInfoMapper;

    @Override
    public boolean saveUserInfo(String username, String password) throws Exception {
        if (username == null|| username.equals("") || username.length() < 3 || password.length() < 4) {
            throw new Exception("参数错误");
        } else {
            QueryWrapper<UserInfo> userInfoQueryWrapper = new QueryWrapper<>();
            userInfoQueryWrapper.lambda().eq(UserInfo::getUsername, username);
//            userInfoQueryWrapper.select("id", "username", "password", "salt").eq("username", username);
            try {
                UserInfo userInfo = userInfoMapper.findUserInfoByUsername(username);
                if (userInfo == null) {
                    System.out.println("增加用户");
//                    ByteSource salt = ByteSource.Util.bytes(username);
                    String newPassword = MD5Util.encode(password, username);
//                    String newPs = new SimpleHash("MD5", password, salt, 2).toHex();
                    userInfo = UserInfo.builder()
                            .username(username)
                            .password(newPassword)
                            .salt(username)
                            .build();

//                    //这里有点问题
//                    List<SysRole> rolelist = new ArrayList<>();
//
//                    SysRole sysRole= sysRoleRepository.findByRole("user");
//                    rolelist.add(sysRole);
//
//                    userInfo.setRoleList(rolelist);
//                    String[] s = role.split(" ");
//                    List<SysRole> sysRoles = new ArrayList<>();
//                    for (String s1 : s) {
//                        if(s1.equals("1")){
//                            throw new UnauthenticatedException("不能手动创建admin用户！");
//                        }
//                        SysRole sysRole = sysRoleRepository.getOne(Long.parseLong(s1));
//                        if (sysRole != null) {
//                            sysRoles.add(sysRole);
//                        }
//                    }
//                    userInfo.setRoleList(sysRoles);
//                    userInfoRepository.save(userInfo);
//                    userLog.setStatus("注册成功");
//                    userLogService.saveLogs(userLog);
                    return userInfoMapper.insert(userInfo) > 0;
                } else {
//                    userLog.setStatus("此用户已存在");
//                    userLogService.saveLogs(userLog);
                    return false;
                }

            } catch (Exception e) {
//                userLog.setStatus("添加失败,系统异常");
//                userLogService.saveLogs(userLog);
                e.printStackTrace();
                throw e;
            }

        }
    }
}
