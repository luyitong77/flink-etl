package com.bdilab.demo.examples;

import com.bdilab.demo.examples.pojo.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class example5_eventtime {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String filePath = "C:\\Users\\hcy\\IdeaProjects\\flink-etl\\flink-test\\file\\sensor.txt";
        // 2. 读入文件数据，得到DataStream
        DataStream<String> inputStream = env.readTextFile(filePath);

        // 3. 转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
//                    @Override
//                    public long extractTimestamp(SensorReading sensorReading) {
//                        return sensorReading.getTimestamp() * 1000L;
//                    }
//                });
            .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                            .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                            .withTimestampAssigner(
                                    (element, recordTimestamp) -> element.getTimestamp() * 1000L
                            )
            );
//        String sinkDDL = "create table inputTable (" +
//                "id varchar," +
//                "ts bigint," +
//                "temp double," +
//                "pt as PROCTIME()" +
//                ") with (" +
//                "'connector.type' = 'filesystem'," +
//                "'format.type' = 'csv'," +
//                "'connector.path' = " + "'" + filePath + "'" +
//                ")";
        Table dataTable = tableEnv.fromDataStream(
                dataStream,
                $("id"),
                $("timestamp"),
                $("temperature").as("temp"),
//                $("pt").proctime()
                $("rt").rowtime()
        );

        dataTable.printSchema();

        tableEnv.toAppendStream(dataTable, Row.class).print();

        env.execute();
    }
}
