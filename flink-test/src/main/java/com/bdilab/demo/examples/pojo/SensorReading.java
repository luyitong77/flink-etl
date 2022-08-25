package com.bdilab.demo.examples.pojo;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTutorial
 * Package: com.atguigu.apitest.beans
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/7 11:34
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName: SensorReading
 * @Description:
 * @Author: wushengran on 2020/11/7 11:34
 * @Version: 1.0
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
// 传感器温度读数的数据类型
public class SensorReading {
    // 属性：id，时间戳，温度值
    private String id;
    private Long timestamp;
    private Double temperature;
}
