package com.bdilab.flinketl.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.gitee.sunchenbin.mybatis.actable.annotation.Column;
import com.gitee.sunchenbin.mybatis.actable.annotation.Table;
import com.gitee.sunchenbin.mybatis.actable.constants.MySqlTypeConstant;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

import java.io.Serializable;

/**
 * description:
 * author: ljw
 * time: 2021/9/16 21:00
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel(value="DatabaseKafka对象", description="")
@Table(name = "database_kafka")
public class DatabaseKafka implements Serializable {

    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.BIGINT, length = 20, isKey = true)
    private Long id;

    @ApiModelProperty(value = "kafka连接的broker list")
    @Column(name = "bootstrap_servers", type = MySqlTypeConstant.VARCHAR, comment = "kafka连接的broker list,例子：master:9092,slave1:9092")
    private String bootstrapServers;

    //kafka的topic
    @ApiModelProperty(value = "kafka的topic")
    @Column(name = "topic_name", type = MySqlTypeConstant.VARCHAR, comment = "kafka的topic")
    private String topicName;

    //下面的主要是注册登录相关的参数,如果没有设置登录限制的可以不填
    //参数可以为空
    @ApiModelProperty(value = "ssl_key_password")
    @Column(name = "ssl_key_password", type = MySqlTypeConstant.VARCHAR, comment = "登录参数，没有可忽略")
    private String sslKeyPassword;

    @ApiModelProperty(value = "ssl_keystore_location")
    @Column(name = "ssl_keystore_location", type = MySqlTypeConstant.VARCHAR, comment = "登录参数，没有可忽略")
    private String sslKeystoreLocation;

    @ApiModelProperty(value = "ssl_keystore_password")
    @Column(name = "ssl_keystore_password", type = MySqlTypeConstant.VARCHAR, comment = "登录参数，没有可忽略")
    private String sslKeystorePassword;

    @ApiModelProperty(value = "ssl_truststore_location")
    @Column(name = "ssl_truststore_location", type = MySqlTypeConstant.VARCHAR, comment = "登录参数，没有可忽略")
    private String sslTruststoreLocation;

    @ApiModelProperty(value = "ssl_truststore_password")
    @Column(name = "ssl_truststore_password", type = MySqlTypeConstant.VARCHAR, comment = "登录参数，没有可忽略")
    private String sslTruststorePassword;

}
