package com.bdilab.flinketl.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.gitee.sunchenbin.mybatis.actable.annotation.Column;
import com.gitee.sunchenbin.mybatis.actable.annotation.Table;
import com.gitee.sunchenbin.mybatis.actable.constants.MySqlTypeConstant;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * @author hcyong
 * @date 2021/7/12
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel(value="UserDatabaseConfig对象", description="")
@Table(name = "user_database_config")
public class UserDatabaseConfig {

    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.INT, length = 11, isKey = true, isAutoIncrement = true)
    private Integer id;

    @ApiModelProperty(value = "数据库连接配置名")
    @Column(name = "config_name", type = MySqlTypeConstant.VARCHAR, comment = "数据库连接配置名")
    private String configName;

    @ApiModelProperty(value = "关联数据库配置的id")
    @Column(name = "fk_database_id", type = MySqlTypeConstant.INT, comment = "关联数据库配置的id")
    private Integer fkDatabaseId;

    @ApiModelProperty(value = "数据库类型")
    @Column(name = "database_type", type = MySqlTypeConstant.INT, comment = "数据库类型 mysql:2 oracle:3 sqlserver:4 Hbase:5 hdfs:6 hive:7 mongoDB:8 excel:9 csv:10 postgresql:11 webapi:12")
    private Integer databaseType;

    @ApiModelProperty(value = "数据库配置描述")
    @Column(name = "description", type = MySqlTypeConstant.VARCHAR, comment = "数据库配置描述")
    private String description;

    @ApiModelProperty(value = "用户账号id")
    @Column(name = "fk_user_id", type = MySqlTypeConstant.INT, comment = "用户账号id")
    private Integer fkUserId;
}
