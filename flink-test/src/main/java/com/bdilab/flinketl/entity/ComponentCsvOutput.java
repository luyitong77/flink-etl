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
 * @since 2021-08-30
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel(value="ComponentCsvOutput对象", description="")
@Table(name = "component_csv_output")
public class ComponentCsvOutput implements Serializable {


    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.INT, length = 11, isKey = true, isAutoIncrement = true)
    private Integer id;

    @ApiModelProperty(value = "hdfs数据源id")
    @Column(name = "fk_hdfs_data_source_id", type = MySqlTypeConstant.INT, comment = "csv数据源id")
    private Integer fkHdfsDataSourceId;

    @ApiModelProperty(value = "保存的文件名")
    @Column(name = "file_name", type = MySqlTypeConstant.VARCHAR, comment = "保存的文件名")
    private String fileName;

    @ApiModelProperty(value = "文件的分隔符")
    @Column(name = "delimiter", type = MySqlTypeConstant.VARCHAR, comment = "文件的分隔符")
    private String delimiter;

    @ApiModelProperty(value = "输出的列名")
    @Column(name = "columns", type = MySqlTypeConstant.VARCHAR, comment = "输出的列名")
    private String columns;

    @ApiModelProperty(value = "输出的列字段属性")
    @Column(name = "columns_type", type = MySqlTypeConstant.VARCHAR, comment = "输出的列字段属性，参考https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/types/")
    private String columnsType;


}
