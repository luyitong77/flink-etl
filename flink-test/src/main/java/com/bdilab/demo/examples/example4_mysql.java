package com.bdilab.demo.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class example4_mysql {
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
        String inputTableSql = "create table inputTable (" +
                "id int," +
                "name varchar," +
                "password varchar," +
                "age int," +
                "primary key(id) not enforced" +
                ") with (" +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:mysql://localhost:3306/test'," +
                "'table-name' = 'student_copy'," +
                "'username' = 'root'," +
                "'password' = '123123'" +
                ")";
        tableEnv.executeSql(inputTableSql);
        Table inputTable = tableEnv.from("inputTable");

        // 3. 查询转换
        // 简单转换
        Table resultTable = inputTable.select($("name"), $("password"), $("age"))
                .filter($("age").isGreater(15));

        // 聚合统计
        Table aggTable = inputTable.groupBy($("name"))
                .select($("name"),
                        $("age").avg().as("avgAge"));

        String outputSql = "create table outputTable (" +
//                "id int," +
                "name varchar," +
                "password varchar," +
                "age int" +
//                "primary key(id) not enforced" +
                ") with (" +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:mysql://localhost:3306/test'," +
                "'table-name' = 'student_copy'," +
                "'username' = 'root'," +
                "'password' = '123123'" +
                ")";
        tableEnv.executeSql(outputSql);

//        resultTable.executeInsert("outputTable");

        String insertSql = "insert into outputTable " +
                "select name, password, age from inputTable " +
                "where age > 15";
        tableEnv.executeSql(insertSql).print();

//        aggTable.executeInsert("outputTable");

        env.execute();
    }
}
