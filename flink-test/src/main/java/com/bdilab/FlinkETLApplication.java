package com.bdilab;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * @author hcyong
 */

@SpringBootApplication
@MapperScan({"com.bdilab.flinketl.mapper", "com.bdilab.demo.examples.mapper", "com.gitee.sunchenbin.mybatis.actable.dao.*"})
@ComponentScan(basePackages = {"com.bdilab.flinketl.*","com.bdilab.demo.*", "com.gitee.sunchenbin.mybatis.actable.manager.*"})
public class FlinkETLApplication {
    public static void main(String[] args) {
        SpringApplication.run(FlinkETLApplication.class, args);
    }
}
