package com.bdilab.demo.examples.kafka;

import lombok.extern.slf4j.Slf4j;
import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

/**
 * @description:
 * @author: ljw
 * @time: 2021/9/16 19:39
 */
@Slf4j
public class KafkaSender {

    private static final String kafkaTopic = "quickstart";
    private static final String brokerAddress = "192.168.0.130:9092";
    private static Properties properties;

    /**
     * 初始化
     */
    private static void init() {
        properties = new Properties();
        properties.put("bootstrap.servers", brokerAddress);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    /**
     * 给Kafka发送信息
     */
    private static void sendUrlToKafka() {
        KafkaProducer producer = new KafkaProducer<String, String>(properties);
        User user = new User();
        for (int i = 0; i < 10; i++) {
            user.setId(i+"");
            user.setName("test-flink-kafka-mysql"+i);
            user.setAge(i+"");
            // 确保发送的消息都是string类型
            String msgContent = JSON.toJSONString(user);
            ProducerRecord record = new ProducerRecord<String, String>(kafkaTopic, null, null, msgContent);
            producer.send(record);
            log.info("send msg:" + msgContent);
        }
        producer.flush();
    }

    public static void main(String[] args) {
        init();
        sendUrlToKafka(); // 发送kafka消息
    }
}
