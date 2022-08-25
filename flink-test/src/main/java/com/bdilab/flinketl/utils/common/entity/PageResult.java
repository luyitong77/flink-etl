package com.bdilab.flinketl.utils.common.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 分页
 * {
 * “success”：“成功”，
 * “code”：10000
 * “message”：“ok”，
 * ”data“：{
 * totalCount：//总条数
 * rows ：//数据列表
 * }
 * }
 */
@Data
@NoArgsConstructor
public class PageResult<T> {

    public PageResult(Long currentSize, Long currentPage, Long totalPage, Long totalCount, List<T> rows) {
        this.currentSize = currentSize;
        this.currentPage = currentPage;
        this.totalPage = totalPage;
        this.totalCount = totalCount;
        this.rows = rows;
    }


    /**
     * 当前页大小
     */
    private Long currentSize;

    /**
     * 当前页码
     */
    private Long currentPage;

    /**
     * 总页数
     */
    private Long totalPage;

    /**
     * 总数
     */
    private Long totalCount;

    // 数据列表
    private List<T> rows;

}
