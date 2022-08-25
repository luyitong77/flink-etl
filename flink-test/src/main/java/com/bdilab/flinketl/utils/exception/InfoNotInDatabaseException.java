package com.bdilab.flinketl.utils.exception;

/**
 * Author: cyz
 * Date: 2019/10/22
 * Description:
 */
public class InfoNotInDatabaseException extends RuntimeException {
    public InfoNotInDatabaseException(String msg){
        super(msg);
    }
}
