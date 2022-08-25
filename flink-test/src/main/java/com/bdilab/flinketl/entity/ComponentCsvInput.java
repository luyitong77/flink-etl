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
@ApiModel(value="ComponentCsvInput对象", description="")
@Table(name = "component_csv_input")
public class ComponentCsvInput implements Serializable {


    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.INT, length = 11, isKey = true, isAutoIncrement = true)
    private Integer id;

    @ApiModelProperty(value = "csv数据源id")
    @Column(name = "fk_data_source_id", type = MySqlTypeConstant.INT, comment = "csv数据源id")
    private Integer fkDataSourceId;

    @ApiModelProperty(value = "文件的分隔符")
    @Column(name = "delimiter", type = MySqlTypeConstant.VARCHAR, comment = "文件的分隔符")
    private String delimiter;

    @ApiModelProperty(value = "输入的列名")
    @Column(name = "columns", type = MySqlTypeConstant.VARCHAR, comment = "输入的列名")
    private String columns;

    @ApiModelProperty(value = "输入的列字段属性")
    @Column(name = "columns_type", type = MySqlTypeConstant.VARCHAR, comment = "输入的列字段属性")
    private String columnsType;


}
