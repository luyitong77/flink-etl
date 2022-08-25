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
 * @since 2021-08-30
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
@ApiModel(value="DatabaseCsv对象", description="")
@Table(name = "database_csv")
public class DatabaseCsv implements Serializable {


    @TableId(value = "id", type = IdType.AUTO)
    @Column(name = "id", type = MySqlTypeConstant.INT, length = 11, isKey = true, isAutoIncrement = true)
    private Integer id;

    @ApiModelProperty(value = "csv文件是否存在")
    @Column(name = "is_exist", type = MySqlTypeConstant.TINYINT, comment = "csv文件是否存在")
    private Boolean isExist;

    @ApiModelProperty(value = "csv文件保存时的实际文件名，包含uuid")
    @Column(name = "file_name", type = MySqlTypeConstant.VARCHAR, comment = "csv文件保存时的实际文件名，包含uuid")
    private String fileName;

    @ApiModelProperty(value = "用户上传时的文件名")
    @Column(name = "user_file_name", type = MySqlTypeConstant.VARCHAR, comment = "用户上传时的文件名")
    private String userFileName;

    @ApiModelProperty(value = "hdfs数据源id")
    @Column(name = "fk_hdfs_data_source_id", type = MySqlTypeConstant.INT, comment = "hdfs数据源id")
    private Integer fkHdfsDataSourceId;

}
