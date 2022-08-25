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
@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel(value="ComponentFilterColumn对象", description="")
@Table(name = "component_filter_column")
public class ComponentFilterColumn implements Serializable {


    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.INT, length = 11, isKey = true, isAutoIncrement = true)
    private Integer id;

    @ApiModelProperty(value = "过滤的列名")
    @Column(name = "column_name", type = MySqlTypeConstant.VARCHAR, comment = "过滤的列名")
    private String columnName;

    @ApiModelProperty(value = "过滤的列字段属性")
    @Column(name = "column_type", type = MySqlTypeConstant.VARCHAR, comment = "过滤的列字段属性")
    private String columnType;

    @ApiModelProperty(value = "过滤的函数，比如大于、小于")
    @Column(name = "filter_function", type = MySqlTypeConstant.VARCHAR, comment = "过滤的函数，比如大于、小于")
    private String filterFunction;

    @ApiModelProperty(value = "过滤的条件，比如大于50")
    @Column(name = "filter_condition", type = MySqlTypeConstant.VARCHAR, comment = "过滤的条件，比如大于50")
    private String filterCondition;

    @ApiModelProperty(value = "并行度")
    @Column(name = "parallelism", type = MySqlTypeConstant.INT, comment = "并行度")
    private int parallelism;


}
