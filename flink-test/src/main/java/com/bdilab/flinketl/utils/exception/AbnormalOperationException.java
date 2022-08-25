package com.bdilab.flinketl.utils.exception;


/**
 * Author: xw
 * Date: 2020/8/12
 * Description:
 */
public class AbnormalOperationException extends RuntimeException{
    public AbnormalOperationException(String msg){
        super(msg);
    }
}
