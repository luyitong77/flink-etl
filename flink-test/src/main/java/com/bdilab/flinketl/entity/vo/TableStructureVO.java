package com.bdilab.flinketl.entity.vo;

import lombok.Data;

/**
 * @Description: 获取指定数据库中表结构
 */
@Data
public class TableStructureVO {

    private String columnName;

    private String columnType;

    private String columnLength;

    private String precision;

    private String extra;

    private String isNullable;

    private String primaryKey;

    private String foreignKey;

    private String columnDefault;

    private String columnComment;

}
