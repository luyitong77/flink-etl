package com.bdilab.flinketl.utils.exception;

/**
 * Author: xw
 * Date: 2020/7/22
 * Description:
 */
public class SqlAntiInjectionException extends RuntimeException {
    public SqlAntiInjectionException (String msg){
        super(msg);
    }
}
