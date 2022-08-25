package com.bdilab.flinketl.utils.common.entity;

import lombok.Data;


/**
 * 数据响应对象
 * {
 * success ：是否成功
 * code    ：返回码
 * message ：返回信息
 * //返回数据
 * data：  ：{
 * <p>
 * }
 * }
 */
@Data
public class ResultData {

    private boolean success;//是否成功
    private Integer code;// 返回码
    private String msg;//返回信息
    private Object data;// 返回数据

    public ResultData() {

    }

    public ResultData(ResultCode code) {
        this.success = code.success;
        this.code = code.code;
        this.msg = code.message;
    }


    public ResultData(ResultCode code, Object data) {
        this.success = code.success;
        this.code = code.code;
        this.msg = code.message;
        this.data = data;
    }

    public ResultData(Integer code, String message, boolean success) {
        this.code = code;
        this.msg = message;
        this.success = success;
    }

    public static ResultData success() {
        return new ResultData(ResultCode.SUCCESS);
    }

    public static ResultData success(Object data) {
        return new ResultData(ResultCode.SUCCESS, data);
    }

    public static ResultData testSuccess(Object data) {
        return new ResultData(ResultCode.TEST_SUCCESS, data);
    }

    public static ResultData error() {
        return new ResultData(ResultCode.SERVER_ERROR);
    }

    public static ResultData fail() {
        return new ResultData(ResultCode.FAIL);
    }

    public static ResultData validatedFail(String errorMessage) {
        ResultData resultData = new ResultData(ResultCode.VALIDATED_FAIL);
        resultData.setMsg(errorMessage);
        return resultData;
    }

}
