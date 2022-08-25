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
 * @since 2021-07-22
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel(value="ComponentSplitColumn对象", description="")
@Table(name = "component_split_column")
public class ComponentSplitColumn implements Serializable {


    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.INT, length = 11, isKey = true, isAutoIncrement = true)
    private Integer id;

    @ApiModelProperty(value = "分隔符，比如：逗号、空格")
    @Column(name = "delimiter", type = MySqlTypeConstant.VARCHAR, comment = "分隔符，比如：逗号、空格")
    private String delimiter;

    @ApiModelProperty(value = "要分割的输入源的列名")
    @Column(name = "split_column_name", type = MySqlTypeConstant.VARCHAR, comment = "要分割的输入源的列名")
    private String splitColumnName;

    @ApiModelProperty(value = "分割后保存字段时输出源对应的列名")
    @Column(name = "save_column_name", type = MySqlTypeConstant.VARCHAR, comment = "分割后保存字段时输出源对应的列名，与输入表的列名顺序对应，用空格分割")
    private String saveColumnName;

    @ApiModelProperty(value = "分割后保存字段时输出源对应的列的类型")
    @Column(name = "save_column_type", type = MySqlTypeConstant.VARCHAR, comment = "分割后保存字段时输出源对应的列的类型，用空格分割")
    private String saveColumnType;

    @ApiModelProperty(value = "并行度")
    @Column(name = "parallelism", type = MySqlTypeConstant.INT, comment = "并行度")
    private Integer parallelism;


}
