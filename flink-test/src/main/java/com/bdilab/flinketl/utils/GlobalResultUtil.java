package com.bdilab.flinketl.utils;

import lombok.Data;

/**
 * @Created with qml
 * @author:qml
 * @Date: 2019/10/11
 * @Time: 17:39
 * @Description  接口返回值统一
 * @edit: cyz
 */
@Data
public class GlobalResultUtil<T> {
    // 响应业务状态
    private Integer code;

    // 响应消息
    private String msg;

    // 响应中的数据
    private T data;


    public static <T>GlobalResultUtil build(Integer code, String msg, T data) {
        return new GlobalResultUtil<>(code, msg, data);
    }

    /**
     * 请求成功返回值，状态码为0
     * @param data 需要返回的数据
     * @return
     */
    public static <T>GlobalResultUtil ok(T data) {
        return new GlobalResultUtil<>(data);
    }

    /**
     * 请求成功返回值，状态码为0
     * @return
     */
    public static GlobalResultUtil ok() {
        return new GlobalResultUtil<>(null);
    }

    /**
     * 服务器端错误返回值
     * @param msg 所要返回的错误信息
     * @return
     */
    public static GlobalResultUtil errorMsg(String msg) {
        return new GlobalResultUtil<>(500, msg, null);
    }

    /**
     * 接口调用错误
     * @param data
     * @return
     */
    public static <T> GlobalResultUtil errorData(T data) {
        return new GlobalResultUtil<>(501, "error", data);
    }

    /**
     * 所请求的东西不存在类异常返回
     * @param data 请求获得的数据
     * @return
     */
    public static <T>GlobalResultUtil inexistence(T data) {
        return new GlobalResultUtil<>(204, "请求信息不存在", data);
    }


    /**
     * 输入参数错误异常返回
     * @param data
     * @param <T>
     * @return
     */
    public static <T>GlobalResultUtil illegalArguments(T data){
        return new GlobalResultUtil<>(407,"输入参数错误",data);
    }

    /**
     * 用户操作错误异常返回
     *
     * @param msg
     * @return
     */
    public static GlobalResultUtil abnormalOperation(String msg){
        return new GlobalResultUtil<>(408,msg,"");
    }


    /**
     * 未登录异常返回
     *
     * @return
     */
    public static GlobalResultUtil notLogin() {
        return new GlobalResultUtil<>(401, "未登录", null);
    }


    /**
     * 未授权异常
     *
     * @return
     */
    public static GlobalResultUtil noAuthorization() {
        return new GlobalResultUtil<>(403, "未授权", null);
    }


//    public static GlobalResultUtil errorTokenMsg(String msg) {
//        return new GlobalResultUtil(502, msg, null);
//    }
//
//    public static GlobalResultUtil errorException(String msg) {
//        return new GlobalResultUtil(555, msg, null);
//    }


    /**
     * 数据库异常时返回值状态码默认为201
     * @param msg
     * @param data
     * @return
     */
    public static <T>GlobalResultUtil databaseErrorException (String msg, T data) {
        return new GlobalResultUtil<>(201, msg, data);
    }
    public GlobalResultUtil() {

    }


    public GlobalResultUtil(Integer code, String msg, T data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }

    public GlobalResultUtil(T data) {
        this.code = 0;
        this.msg = "OK";
        this.data = data;
    }

    public Boolean isOK() {
        return this.code == 0;
    }

    public GlobalResultUtil<T> replaceAll(String s, String s1) {
        return GlobalResultUtil.ok();

    }
}
