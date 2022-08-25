package com.bdilab.flinketl.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.gitee.sunchenbin.mybatis.actable.annotation.Column;
import com.gitee.sunchenbin.mybatis.actable.constants.MySqlTypeConstant;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

import javax.persistence.Table;
import java.io.Serializable;

/**
 * <p>
 * 数据库配置表
 * </p>
 *
 * @author ljw
 * @since 2021-07-28
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
@Table(name = "database_oracle")
@ApiModel(value="DatabaseOracle对象", description="数据库配置表")
public class DatabaseOracle implements Serializable {

    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.BIGINT, length = 20, isKey = true)
    private Long id;

    @ApiModelProperty(value = "数据库名称")
    @Column(name = "database_name", type = MySqlTypeConstant.VARCHAR, comment = "连接的数据库名")
    private String databaseName;

    @ApiModelProperty(value = "数据库地址")
    @Column(name = "hostname", type = MySqlTypeConstant.VARCHAR, comment = "数据库主机名")
    private String hostname;

    @ApiModelProperty(value = "登录密码")
    @Column(name = "password", type = MySqlTypeConstant.VARCHAR, comment = "数据库用户密码")
    private String password;

    @ApiModelProperty(value = "数据库对应的端口号")
    @Column(name = "port", type = MySqlTypeConstant.INT, comment = "数据库端口号")
    private int port;

    @ApiModelProperty(value = "登录用户名")
    @Column(name = "username", type = MySqlTypeConstant.VARCHAR, comment = "数据库用户名")
    private String username;

    @ApiModelProperty(value = "数据库名是否是服务名 1：服务名 0：sid")
    @Column(name = "is_service_name", type = MySqlTypeConstant.INT, comment = "数据库名是否是服务名 1：服务名 0：sid")
    private int isServiceName;


}
