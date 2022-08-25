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
@NoArgsConstructor
@AllArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel(value="ComponentTableInput对象", description="")
@Table(name = "component_table_input")
public class ComponentTableInput implements Serializable {


    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.INT, length = 11, isKey = true, isAutoIncrement = true)
    private Integer id;

    @ApiModelProperty(value = "数据源id")
    @Column(name = "fk_data_source_id", type = MySqlTypeConstant.INT, comment = "数据源id")
    private Integer fkDataSourceId;

    @ApiModelProperty(value = "数据库表名")
    @Column(name = "table_name", type = MySqlTypeConstant.VARCHAR, comment = "数据库表名")
    private String tableName;

    @ApiModelProperty(value = "输入的列名")
    @Column(name = "columns", type = MySqlTypeConstant.VARCHAR, comment = "输入的列名")
    private String columns;

    @ApiModelProperty(value = "输入的列字段属性")
    @Column(name = "columns_type", type = MySqlTypeConstant.VARCHAR, comment = "输入的列字段属性")
    private String columnsType;

    @ApiModelProperty(value = "自定义sql查询语句")
    @Column(name = "custom_sql", type = MySqlTypeConstant.VARCHAR, comment = "自定义sql查询语句")
    private String customSql;
}
