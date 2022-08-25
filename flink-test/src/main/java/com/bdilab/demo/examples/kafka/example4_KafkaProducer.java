package com.bdilab.demo.examples.kafka;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @description:
 * @author: ljw
 * @time: 2021/9/27 20:03
 */
public class example4_KafkaProducer {

    public static void main(String[] args) throws Exception {

        int parallelism = 1;

        String[] types = {"varchar", "varchar", "varchar"};
        String[] columns = {"id", "name", "age"};

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hcyong1:9092");
        String topicId = "newtest";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(parallelism);

        DataStream<Row> dataStream = env.addSource(new MySQLSource());

        dataStream.addSink(new FlinkKafkaProducer<>(
                topicId,
                new RowSerializationSchema(new Tuple2<>(types, columns)),
                properties));

        dataStream.print();

        env.execute();
    }

}
