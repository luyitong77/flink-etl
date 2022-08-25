package com.bdilab.demo.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class example2_inputFile {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        // 1.1 基于老版本planner的流处理
//        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
//                .useOldPlanner()
//                .inStreamingMode()
//                .build();
//
//        // 1.2 基于老版本planner的批处理
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);
//
//        // 1.3 基于Blink的流处理
//        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);
//
//        // 1.4 基于Blink的批处理
//        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inBatchMode()
//                .build();
//        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);

        // 2. 表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        String filePath = "C:\\Users\\hcy\\IdeaProjects\\flink-etl\\flink-test\\file\\sensor.txt";
        String sql = "create table inputTable (" +
                "id varchar," +
                "ts bigint," +
                "temp double) with (" +
                "'connector.type' = 'filesystem'," +
                "'format.type' = 'csv'," +
                "'connector.path' = " + "'" + filePath + "')";
        tableEnv.executeSql(sql);
        Table inputTable = tableEnv.from("inputTable");

        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select($("id"), $("temp"))
                .filter($("id").isEqual("sensor_6"));

        // 3.2 聚合统计
        Table aggTable = inputTable.groupBy($("id"))
                .select($("id"),
                        $("id").count().as("count"),
                        $("temp").avg().as("avgTemp"));

        // 3.2 SQL
        tableEnv.sqlQuery("select id, temp from inputTable where id = 'sensor_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");

        resultTable.printSchema();
        // 打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(aggTable, Row.class).print("agg");
        tableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlagg");

        env.execute();
    }

}
