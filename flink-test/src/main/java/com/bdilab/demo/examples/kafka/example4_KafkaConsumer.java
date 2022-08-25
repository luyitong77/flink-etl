package com.bdilab.demo.examples.kafka;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @description:
 * @author: ljw
 * @time: 2021/9/27 20:36
 */
public class example4_KafkaConsumer {
    public static void main(String[] args) throws Exception {

        int parallelism = 1;
        String[] types = {"varchar", "varchar", "varchar"};
        String[] columns = {"id", "name", "age"};

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hcyong2:9092");
//        properties.setProperty("group.id", "test");
        String topicId = "newtest";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(parallelism);

        DataStreamSource<Row> dataStream = env.addSource(
            new FlinkKafkaConsumer<>(
                    topicId,
                    new RowDeserializationSchema(new Tuple2<>(types, columns)),
                    properties
            )
        );

        dataStream.print();

        //Kafka实时消费数据到Kafka
//        dataStream.addSink(new FlinkKafkaProducer<>(
//                "hcyong3:9092",
//                "usertest",
//                new RowSchema(new Tuple2<>(types, columns)))
//        );

        //Kafka--MySQL
        dataStream.addSink(new MySQLSink());

        env.execute();

    }
}
