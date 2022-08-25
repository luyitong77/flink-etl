package com.bdilab.flinketl.utils;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: xw
 * Date: 2020/7/14
 * Description:
 */
public class PageBean<T> implements Serializable{

    private static final long serialVersionUID = 5760097915453738435L;
    public static final int DEFAULT_PAGE_SIZE = 10;
    /**
     * 每页显示个数
     */
    private int pageSize;
    /**
     * 当前页数
     */
    private int currentPage;
    /**
     * 总页数
     */
    private int totalPage;
    /**
     * 总记录数
     */
    private int totalCount;
    /**
     * 结果列表
     */
    private List<T> pageDatas;

    /**
     * 默认获取第一页
     */
    public PageBean(){
        this.currentPage = 1;
        this.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * 获取当前页
     */
    public PageBean(int currentPage, int pageSize){
        this.currentPage=currentPage<=0?1:currentPage;
        this.pageSize=pageSize<=0?1:pageSize;
    }


    public int getPageSize() {
        return pageSize;
    }
    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getCurrentPage() {
        return currentPage;
    }
    public void setCurrentPage(int currentPage) {
        this.currentPage = currentPage;
    }

    public int getTotalPage() {
        return totalPage;
    }
    public void setTotalPage(int totalPage) {
        this.totalPage = totalPage;
    }

    public int getTotalCount() {
        return totalCount;
    }
    public void setTotalCount(int totalCount) {
        //设置了totalCount就可以计算出总totalPage
        this.totalCount = totalCount;
        int countRecords = this.getTotalCount();
        int totalPages = countRecords % pageSize == 0 ? countRecords / pageSize : (countRecords / pageSize + 1);
        setTotalPage(totalPages);
    }

    /**
     * 设置结果 及总页数
     * @param initialDatas 需要分页的初始数据
     */
    public void build(List<T> initialDatas) {
        int total = initialDatas.size();
        setTotalCount(total);

        int start = (currentPage-1)*pageSize;
        int end = start+pageSize > totalCount? totalCount : start+pageSize;
        int index;
        List<T> datas=new ArrayList<>();
        for(index = start ; index<end ; index++){
            datas.add(initialDatas.get(index));
        }
        setPageDatas(datas);

    }
    public List<T> getPageDatas() {
        return pageDatas;
    }
    public void setPageDatas(List<T> pageDatas) {
        this.pageDatas = pageDatas;
    }

    public boolean isFirst()
    {
        return (this.currentPage == 1);
    }

    public boolean isLast() {
        return (this.currentPage == getTotalPage());
    }
}
