package com.bdilab.flinketl.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import java.io.Serializable;

import com.gitee.sunchenbin.mybatis.actable.annotation.Column;
import com.gitee.sunchenbin.mybatis.actable.annotation.Table;
import com.gitee.sunchenbin.mybatis.actable.constants.MySqlTypeConstant;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.*;

/**
 * <p>
 * 
 * </p>
 *
 * @author hcyong
 * @since 2021-09-08
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel(value="DatabaseHdfs对象", description="")
@Table(name = "database_hdfs")
public class DatabaseHdfs implements Serializable {


    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.INT, length = 11, isKey = true, isAutoIncrement = true)
    private Integer id;

    @ApiModelProperty(value = "hdfs的主机号")
    @Column(name = "hdfs_ip", type = MySqlTypeConstant.VARCHAR, comment = "hdfs的主机号")
    private String hdfsIp;

    @ApiModelProperty(value = "hdfs的端口号")
    @Column(name = "hdfs_port", type = MySqlTypeConstant.INT, comment = "hdfs的端口号")
    private Integer hdfsPort;


}
