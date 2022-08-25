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
@ApiModel(value="ComponentTableUpsert对象", description="")
@Table(name = "component_table_upsert")
public class ComponentTableUpsert implements Serializable {


    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.INT, length = 11, isKey = true, isAutoIncrement = true)
    private Integer id;

    @ApiModelProperty(value = "数据源id")
    @Column(name = "fk_data_source_id", type = MySqlTypeConstant.INT, comment = "数据源id")
    private Integer fkDataSourceId;

    @ApiModelProperty(value = "数据库表名")
    @Column(name = "table_name", type = MySqlTypeConstant.VARCHAR, comment = "数据库表名")
    private String tableName;

    @ApiModelProperty(value = "输出的列名")
    @Column(name = "columns", type = MySqlTypeConstant.VARCHAR, comment = "输出的列名")
    private String columns;

    @ApiModelProperty(value = "输出的列字段属性")
    @Column(name = "columns_type", type = MySqlTypeConstant.VARCHAR, comment = "输出的列字段属性")
    private String columnsType;

    @ApiModelProperty(value = "对应列名是否作为比较流字段，比如Y N Y Y")
    @Column(name = "compare_flag", type = MySqlTypeConstant.VARCHAR, comment = "对应列名是否作为比较流字段，比如Y N Y Y")
    private String compareFlag;

    @ApiModelProperty(value = "对应列名是否更新，比如Y N Y Y")
    @Column(name = "upsert_flag", type = MySqlTypeConstant.VARCHAR, comment = "对应列名是否更新，比如Y N Y Y")
    private String upsertFlag;

    @ApiModelProperty(value = "是否支持以及采用分区,0 无分区，1 有分区")
    @Column(name = "is_partitioned", type = MySqlTypeConstant.TINYINT, comment = "是否支持以及采用分区,0 无分区，1 有分区")
    private int isPartitioned=0;

    @ApiModelProperty(value = "分区列名")
    @Column(name = "partiton_col", type = MySqlTypeConstant.VARCHAR, comment = "分区列名")
    private String partitonCol=null;

    @ApiModelProperty(value = "分区列名属性")
    @Column(name = "partitiion_col_type", type = MySqlTypeConstant.VARCHAR, comment = "分区列名属性")
    private String partitiionColType=null;

}
