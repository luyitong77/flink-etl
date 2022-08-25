package com.bdilab.flinketl.utils.kafka;

import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class KafkaSenderForTest {

    public static final String broker_list = "hcyong1:9092";
    public static final String topic = "newtest";  // kafka topic，Flink 程序中需要和这个统一
    public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "kafka.RowSerializerOnlyForKafkaProducer"); //value 序列化
        KafkaProducer<String, Row> producer = new KafkaProducer<>(props);

        Row row = Row.withNames();
//        row.setField("id", id);
//        row.setField("score", "score");
//        row.setField("name", "name");
        row.setField("score", String.valueOf(new Random().nextInt(100)) );
        row.setField("name", sdf.format(new Date(System.currentTimeMillis())));
        row.setField("sex", "test");

        ProducerRecord<String, Row> record = new ProducerRecord<>(topic, null, null, row);
        System.out.println("发送数据: " + record.toString());
        producer.send(record);

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            //current: 500ms
            Thread.sleep(500L);
            writeToKafka();
        }
    }
}
