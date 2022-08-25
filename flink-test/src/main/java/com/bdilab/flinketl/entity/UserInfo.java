package com.bdilab.flinketl.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.gitee.sunchenbin.mybatis.actable.annotation.Column;
import com.gitee.sunchenbin.mybatis.actable.annotation.Table;
import com.gitee.sunchenbin.mybatis.actable.constants.MySqlTypeConstant;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

import java.io.Serializable;

/**
 * <p>
 * 
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel(value="UserInfo对象", description="")
@Table(name = "user_info")
public class UserInfo implements Serializable {


    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.INT, length = 11, isKey = true, isAutoIncrement = true)
    private Integer id;

    @ApiModelProperty(value = "用户名")
    @Column(name = "username", type = MySqlTypeConstant.VARCHAR, comment = "用户名")
    private String username;

    @ApiModelProperty(value = "用户密码")
    @Column(name = "password", type = MySqlTypeConstant.VARCHAR, comment = "用户密码")
    private String password;

    @ApiModelProperty(value = "密码盐")
    @Column(name = "salt", type = MySqlTypeConstant.VARCHAR, comment = "密码盐")
    private String salt;


}
