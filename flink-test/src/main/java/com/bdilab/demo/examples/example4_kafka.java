package com.bdilab.demo.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class example4_kafka {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 2. 连接kafka，读取数据
//        String filePath = "C:\\Users\\hcy\\IdeaProjects\\flink-etl\\flink-test\\file\\sensor.txt";
//        String outputPath = "C:\\Users\\hcy\\IdeaProjects\\flink-etl\\flink-test\\file\\output.txt";
        /**
         *           String sinkDDL = "create table sinkTable(
         *                               a int,
         *                               b varchar
         *                             ) with (
         *                               'connector.type' = 'filesystem',
         *                               'format.type' = 'csv',
         *                               'connector.path' = 'xxx'
         *                             )";
         *
         *           "create table outputTable(" +
         *                 "`id` STRING," +
         *                 "`temp` DOUBLE" +
         *             ")with(" +
         *                 "'connector' = 'kafka'," +
         *                 "'topic' = 'sensor_simple'," +
         *                 "'properties.zookeeper.connect' = '192.168.56.150:2181'," +
         *                 "'properties.bootstrap.servers' = '192.168.56.150:9092'," +
         *                 "'properties.group.id' = 'testGroup'," +
         *                 "'scan.startup.mode' = 'earliest-offset'," +
         *                 "'format' = 'csv'," +
         *                 "'csv.field-delimiter' = ','" +
         *
         */
        String topicInput = "sensor";
        String bootstrapServers = "zookeeper1:9092";
        String zookeeperConnect = "zookeeper1:2181";
        String inputTableSql = "create table inputTable (" +
                "id varchar," +
                "ts bigint," +
                "temp double) with (" +
                "'connector' = 'kafka'," +
                "'format' = 'csv'," +
                "'csv.field-delimiter' = ','," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'topic' = '" + topicInput + "'," +
                "'properties.group.id' = 'testGroup'," +
                "'properties.bootstrap.servers' = '" + bootstrapServers + "'," +
                "'properties.zookeeper.connect' = '" + zookeeperConnect + "')";
        tableEnv.executeSql(inputTableSql);
        Table inputTable = tableEnv.from("inputTable");

        // 3. 查询转换
        // 简单转换
        Table resultTable = inputTable.select($("id"), $("temp"))
                .filter($("id").isEqual("sensor_6"));

        // 聚合统计
        Table aggTable = inputTable.groupBy($("id"))
                .select($("id"),
                        $("id").count().as("count"),
                        $("temp").avg().as("avgTemp"));


        String topicOutput = "sinktest";
        String outputSql = "create table outputTable (" +
                "id varchar," +
//                "ts bigint," +
                "temp double) with (" +
                "'connector' = 'kafka'," +
                "'format' = 'csv'," +
                "'csv.field-delimiter' = ','," +
//                "'scan.startup.mode' = 'earliest-offset'," +
                "'topic' = '" + topicOutput + "'," +
                "'properties.group.id' = 'testGroup'," +
                "'properties.bootstrap.servers' = '" + bootstrapServers + "'," +
                "'properties.zookeeper.connect' = '" + zookeeperConnect + "')";
        tableEnv.executeSql(outputSql);

        resultTable.executeInsert("outputTable");

//        // 聚合操作涉及旧数据的修改更新，文件系统不支持
//        aggTable.executeInsert("outputTable");

        env.execute();
    }
}
