package com.bdilab.demo.examples.service;

import com.bdilab.flinketl.utils.GlobalResultUtil;

/**
 * @author hcyong
 * @date 2021/7/12
 */
public interface TestService {
    GlobalResultUtil<Boolean> testFlink() throws Exception;
    GlobalResultUtil<Boolean> testFlink1() throws Exception;

}
