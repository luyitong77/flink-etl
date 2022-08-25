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
 * 数据库database_mysql实体类
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
@ApiModel(value="DatabaseSqlserver对象", description="")
@Table(name = "database_sqlserver")
public class DatabaseSqlserver implements Serializable {

    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.INT, length = 11, isKey = true, isAutoIncrement = true)
    private Integer id;

    @ApiModelProperty(value = "连接的数据库名")
    @Column(name = "database_name", type = MySqlTypeConstant.VARCHAR, comment = "连接的数据库名")
    private String databaseName;

    @ApiModelProperty(value = "数据库主机名")
    @Column(name = "hostname", type = MySqlTypeConstant.VARCHAR, comment = "数据库主机名")
    private String hostname;

    @ApiModelProperty(value = "数据库端口号")
    @Column(name = "port", type = MySqlTypeConstant.INT, comment = "数据库端口号")
    private int port;

    @ApiModelProperty(value = "数据库用户名")
    @Column(name = "username", type = MySqlTypeConstant.VARCHAR, comment = "数据库用户名")
    private String username;

    @ApiModelProperty(value = "数据库用户密码")
    @Column(name = "password", type = MySqlTypeConstant.VARCHAR, comment = "数据库用户密码")
    private String password;

}
