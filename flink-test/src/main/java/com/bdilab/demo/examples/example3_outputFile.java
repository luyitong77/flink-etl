package com.bdilab.demo.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class example3_outputFile {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 2. 表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        String filePath = "C:\\Users\\hcy\\IdeaProjects\\flink-etl\\flink-test\\file\\sensor.txt";
        String outputPath = "C:\\Users\\hcy\\IdeaProjects\\flink-etl\\flink-test\\file\\output.txt";
        String inputTableSql = "create table inputTable (" +
                "id varchar," +
                "ts bigint," +
                "temp double) with (" +
                "'connector.type' = 'filesystem'," +
                "'format.type' = 'csv'," +
                "'connector.path' = " + "'" + filePath + "')";
        tableEnv.executeSql(inputTableSql);
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

        String outputTableSql = "create table outputTable (" +
                "id varchar," +
//                "ts bigint," +
                "temp double) with (" +
                "'connector.type' = 'filesystem'," +
                "'format.type' = 'csv'," +
                "'connector.path' = " + "'" + outputPath + "')";
        tableEnv.executeSql(outputTableSql);

        resultTable.executeInsert("outputTable");

//        // 聚合操作涉及旧数据的修改更新，文件系统不支持
//        aggTable.executeInsert("outputTable");

        env.execute();
    }
}
