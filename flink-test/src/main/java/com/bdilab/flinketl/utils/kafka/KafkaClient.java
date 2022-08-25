package com.bdilab.flinketl.utils.kafka;

import com.bdilab.flinketl.entity.ComponentTableInput;
import com.bdilab.flinketl.entity.DatabaseKafka;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.springframework.security.core.parameters.P;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @description:
 * @author: ljw
 * @time: 2021/9/16 21:00
 */
public class KafkaClient {

    /**
     * 返回值：
     * class java.lang.Integer
     * class java.lang.String
     * class java.lang.Float
     * class java.lang.Long
     * @param object 变量
     * @return 变量类型名全称
     */

    private static final String YAML_PATH = "C:\\Users\\ljw\\IdeaProjects\\flink-etl\\flink-test\\src\\main\\resources\\application.yaml";
    private static final String COMPONENT_TABLE_INPUT = "component_table_input";
    private static final String DATABASE_KAFKA = "database_kafka";


    public KafkaClient() {}






}
