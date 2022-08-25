package com.bdilab.flinketl.utils.kafka;

import com.bdilab.flinketl.mapper.SysCommonTaskMapper;
import com.bdilab.flinketl.service.DatabaseKafkaService;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @description:
 * @author: ljw
 * @time: 2021/10/19 20:37
 */

@Slf4j
@Component
public class FlinkKafkaUtil {
    @Resource
    SysCommonTaskMapper commonTaskMapper;
    @Resource
    DatabaseKafkaService databaseKafkaService;


//    public SingleOutputStreamOperator<Row> generateKafkaDataSource(StreamExecutionEnvironment env, int sourceInputId) throws Exception {
//
//    }
}
