package com.bdilab.flinketl.utils.exception;

/**
 * Author: cyz
 * Date: 2019/10/26
 * Description:
 */
public class ElementNotExistException extends RuntimeException {
    public ElementNotExistException(String msg){
        super(msg);
    }
}
