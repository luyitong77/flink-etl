package com.bdilab.flinketl.utils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Author: cyz
 * Date: 2020/8/21
 * Description:
 */
public class SolveReturnResults {

    public static Map<String,Object> truncateResults(List<String> results){
        Map<String,Object> resultMap = new HashMap<>();
        if (results.size() >= 100){
            resultMap.put("fieldNames",results.subList(0,100));
            resultMap.put("isMore",true);
        }else {
            resultMap.put("fieldNames",results);
            resultMap.put("isMore",false);
        }
        return resultMap;
    }


    public static List<String> searchResults(List<String> results,String searchName){
        List<String> searchResults = new ArrayList<>();
        if(searchName == null){
            return searchResults;
        }
        Pattern pattern = Pattern.compile(searchName,Pattern.CASE_INSENSITIVE);
        for (String element : results) {
            Matcher matcher = pattern.matcher(element);
            if (matcher.find()){
                searchResults.add(element);
            }
        }
        return searchResults;
    }

    public static PageBean<String> returnPageFromResults(List<String> fields, int currentPage, int pageSize){
        PageBean<String> pageBean = new PageBean<>();
        pageBean.setCurrentPage(currentPage);
        pageBean.setPageSize(pageSize);
        pageBean.setTotalCount(fields.size());
        if (currentPage < 1 || pageSize < 1){
            return pageBean;
        }
        int start = (currentPage - 1) * pageSize;
        if (start >= fields.size()){
            pageBean.setPageDatas(null);
        }else {
            int end = (start + pageSize) > fields.size() ? fields.size() : (start + pageSize);
            pageBean.setPageDatas(fields.subList(start,end));
        }
        int totalPage = (fields.size() % pageSize) == 0 ? fields.size() / pageSize
                : (fields.size() / pageSize + 1);
        pageBean.setTotalPage(totalPage);
        return pageBean;
    }

//    public static void main(String[] args) {
//        List<String> test = Arrays.asList(new String[]{"abc","a","a","a","a","a","a","a","a","a","a","a"});
//        System.out.println(returnPageFromResults(test,2,11));
//    }
}
