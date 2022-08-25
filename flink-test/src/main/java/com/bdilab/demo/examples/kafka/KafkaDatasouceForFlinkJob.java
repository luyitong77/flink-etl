package com.bdilab.demo.examples.kafka;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @description:
 * @author: ljw
 * @time: 2021/9/16 19:41
 */


import java.util.Properties;

/**
 * kafka作为数据源，消费kafka中的消息
 */
public class KafkaDatasouceForFlinkJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.0.131:9092");
//        properties.put("zookeeper.connect","192.168.100.10:2181");
        properties.put("group.id","test-group");
        properties.put("auto.offset.reset","latest");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        DataStreamSource<String> dataStreamSource = env.addSource(
                new FlinkKafkaConsumer<String>(
                        // topic
                        "quickstart" ,
                        new SimpleStringSchema(),
                        properties
                )
        ).setParallelism(1);

        DataStream<Tuple3<String, String, String>> sourceStreamTra = dataStreamSource.map(
            new MapFunction<String, Tuple3<String, String, String>>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Tuple3<String, String, String> map(String value) {
                    User user   = JSON.parseObject(value,User.class);
                    return new Tuple3<>(user.getId(),user.getName(),user.getAge());
                }
            }
        );

        //传入数据库
//        sourceStreamTra.addSink(new MySQLSink());
        env.execute("Flink add kafka data source");
    }

    public static void run() {

    }
}

