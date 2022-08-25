package com.bdilab.flinketl.utils.exception;

/**
 * Author: cyz
 * Date: 2019/10/22
 * Description:
 */
public class DatabaseNotExistException extends RuntimeException {
    public DatabaseNotExistException(String msg){
        super(msg);
    }
}
