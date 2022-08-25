package com.bdilab.flinketl.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import java.io.Serializable;

import com.gitee.sunchenbin.mybatis.actable.annotation.Column;
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
 * @since 2021-09-09
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel(value="ComponentHdfsInput对象", description="")
public class ComponentHdfsInput implements Serializable {


    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.INT, length = 11, isKey = true, isAutoIncrement = true)
    private Integer id;

    @ApiModelProperty(value = "hdfs数据源id")
    @Column(name = "fk_data_source_id", type = MySqlTypeConstant.INT, comment = "hdfs数据源id")
    private Integer fkDataSourceId;

    @ApiModelProperty(value = "hdfs文件输入的路径")
    @Column(name = "file_path", type = MySqlTypeConstant.VARCHAR, comment = "hdfs文件输入的路径")
    private String filePath;

    @ApiModelProperty(value = "文件字段的名称，用空格分开")
    @Column(name = "columns", type = MySqlTypeConstant.VARCHAR, comment = "文件字段的名称，用空格分开")
    private String columns;

    @ApiModelProperty(value = "文件的分隔符")
    @Column(name = "delimiter", type = MySqlTypeConstant.VARCHAR, comment = "文件的分隔符")
    private String delimiter;


}
