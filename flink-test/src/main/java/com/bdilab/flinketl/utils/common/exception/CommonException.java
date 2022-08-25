package com.bdilab.flinketl.utils.common.exception;

import com.bdilab.flinketl.utils.common.entity.ResultCode;
import lombok.Data;

@Data
public class CommonException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private ResultCode code = ResultCode.SERVER_ERROR;

    //是否成功
    private boolean success = false;

    // 返回码
    private Integer codeNum;

    // 返回信息
    private String msg;

    public CommonException() {
    }

    public CommonException(ResultCode resultCode) {
        super(resultCode.message());
        this.code = resultCode;
        this.codeNum = resultCode.code();
        this.msg = resultCode.message();
    }

    /**
     * 用于自定义的错误信息
     *
     * @param resultCode
     * @param msg
     */
    public CommonException(ResultCode resultCode, String msg) {
        super(resultCode.message());
        this.code = resultCode;
        this.codeNum = resultCode.code();
        this.msg = msg;
    }
}
