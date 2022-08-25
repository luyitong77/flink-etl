package com.bdilab.demo.examples.service.impl;

import com.bdilab.demo.examples.service.TestService;
import com.bdilab.flinketl.utils.GlobalResultUtil;
import com.bdilab.flinketl.utils.ResultExecuter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author hcyong
 * @date 2021/7/12
 */
@Service
public class TestServiceImpl implements TestService {

    @Value("${flink.ip}")
    String ip;
    @Value("${flink.port}")
    int port;
    @Value("${flink.jarPath}")
    String jarPath;

    String filePath = "/home/hcyong/Documents/sensor.txt";
    String outputPath = "/home/hcyong/Documents/output.txt";

//    String filePath = "C:\\Users\\hcy\\IdeaProjects\\flink-etl\\flink-test\\file\\sensor.txt";
//    String outputPath = "C:\\Users\\hcy\\IdeaProjects\\flink-etl\\flink-test\\file\\output.txt";
    @Override
    public GlobalResultUtil<Boolean> testFlink() {

        File file = new File(outputPath);
        if (file.exists()) {
            System.out.println("delete output.txt");
            if (file.delete()) {
                System.out.println("delete success");
            } else {
                System.out.println("delete fail");
            }
        }

        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run() throws Exception {
                flinkFile();
                return true;
            }
        }.execute();


//        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");


//        resultTable.executeInsert("outputTable");

//        // 聚合操作涉及旧数据的修改更新，文件系统不支持
//        aggTable.executeInsert("outputTable");

//        env.execute();
    }

    public void flinkFile() throws Exception {

        // 创建环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(ip, port, jarPath);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 表的创建：连接外部系统，读取数据
        // 2.1 读取文件


        String inputTableSql = "create table inputTable (" +
                "id varchar," +
                "ts bigint," +
                "temp double) with (" +
                "'connector.type' = 'filesystem'," +
                "'format.type' = 'csv'," +
                "'connector.path' = " + "'" + filePath + "')";
        tableEnv.executeSql(inputTableSql);
        Table inputTable = tableEnv.from("inputTable");

        String outputTableSql = "create table outputTable (" +
                "id varchar," +
//                "ts bigint," +
                "temp double) with (" +
                "'connector.type' = 'filesystem'," +
                "'format.type' = 'csv'," +
                "'connector.path' = " + "'" + outputPath + "')";
        tableEnv.executeSql(outputTableSql);

        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select($("id"), $("temp"))
                .filter($("temp").isGreater(33.0));

        // 3.2 聚合统计
        Table aggTable = inputTable.groupBy($("id"))
                .select($("id"),
                        $("id").count().as("count"),
                        $("temp").avg().as("avgTemp"));

        resultTable.executeInsert("outputTable");

//        env.execute();

        // 3.2 SQL
//        return tableEnv.executeSql("insert into outputTable select id, temp from inputTable where temp > 33");
    }

    @Override
    public GlobalResultUtil<Boolean> testFlink1() throws Exception {
        return new ResultExecuter<Boolean>(){
            @Override
            public Boolean run() throws Exception {
                mysqlTable();
                return true;
            }
        }.execute();
    }

    public void mysqlTable() throws Exception {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(ip, port, jarPath);
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
                "'url' = 'jdbc:mysql://192.168.0.239:3306/test'," +
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
                "'url' = 'jdbc:mysql://192.168.0.239:3306/test'," +
                "'table-name' = 'student_copy'," +
                "'username' = 'root'," +
                "'password' = '123123'" +
                ")";
        tableEnv.executeSql(outputSql);

        resultTable.executeInsert("outputTable");

//        String insertSql = "insert into outputTable " +
//                "select name, password, age from inputTable " +
//                "where age > 15";
//        tableEnv.executeSql(insertSql).print();

//        aggTable.executeInsert("outputTable");

//        env.execute();
    }
}
