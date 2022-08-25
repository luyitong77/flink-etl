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
@ApiModel(value = "SysCommonTask对象", description = "")
@Table(name = "sys_common_task")
public class SysCommonTask implements Serializable {


    @ApiModelProperty(value = "任务id")
    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.INT, length = 11, isKey = true, isAutoIncrement = true)
    private Integer id;

    @ApiModelProperty(value = "输入组件id")
    @Column(name = "fk_input_id", type = MySqlTypeConstant.INT, comment = "输入组件id")
    private Integer fkInputId;

    @ApiModelProperty(value = "输入组件类型")
    @Column(name = "source_data_type", type = MySqlTypeConstant.VARCHAR, comment = "输入组件类型")
    private String sourceDataType;

    @ApiModelProperty(value = "输出组件id")
    @Column(name = "fk_upsert_id", type = MySqlTypeConstant.INT, comment = "输出组件id")
    private Integer fkUpsertId;

    @ApiModelProperty(value = "输出组件类型")
    @Column(name = "target_data_type", type = MySqlTypeConstant.VARCHAR, comment = "输出组件类型")
    private String targetDataType;

    @ApiModelProperty(value = "过滤组件id")
    @Column(name = "fk_filter_column_id", type = MySqlTypeConstant.INT, comment = "过滤组件id")
    private Integer fkFilterColumnId;

    @ApiModelProperty(value = "分割组件id")
    @Column(name = "fk_split_column_id", type = MySqlTypeConstant.INT, comment = "分割组件id")
    private Integer fkSplitColumnId;

    @ApiModelProperty(value = "是否提交")
    @Column(name = "is_commit", type = MySqlTypeConstant.TINYINT, comment = "是否提交(未提交：0 提交：1)")
    private boolean isCommit;

    @ApiModelProperty(value = "任务名")
    @Column(name = "task_name", type = MySqlTypeConstant.VARCHAR, comment = "任务名")
    private String taskName;

    @ApiModelProperty(value = "任务状态")
    @Column(name = "task_status", type = MySqlTypeConstant.INT, comment = "任务状态：0：未执行，1：执行中，2：暂停中，3：已停止， 4：已结束")
    private Integer taskStatus;

    @ApiModelProperty(value = "组件顺序")
    @Column(name = "step_order", type = MySqlTypeConstant.VARCHAR, comment = "转换组件步骤的顺序 步骤名依次用空格分隔")
    private String stepOrder;

    @ApiModelProperty(value = "并行度")
    @Column(name = "parallelism", type = MySqlTypeConstant.INT, comment = "并行度")
    private Integer parallelism;

    @ApiModelProperty(value = "flink任务id")
    @Column(name = "job_id", type = MySqlTypeConstant.VARCHAR, comment = "flink任务id")
    private String jobId;
}
