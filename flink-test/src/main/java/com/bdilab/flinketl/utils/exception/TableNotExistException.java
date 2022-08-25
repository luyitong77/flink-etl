package com.bdilab.flinketl.utils.exception;

/**
 * Author: cyz
 * Date: 2019/10/22
 * Description:
 */
public class TableNotExistException extends RuntimeException {
    public TableNotExistException(String msg){
        super(msg);
    }
}
