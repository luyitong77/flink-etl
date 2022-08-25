import com.alibaba.fastjson.JSONObject;
import com.bdilab.FlinkETLApplication;
import com.bdilab.flinketl.flink.entity.FlinkJobs;
import com.bdilab.demo.examples.pojo.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.web.client.RestTemplate;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;

@RunWith(SpringJUnit4ClassRunner.class)
@EnableAutoConfiguration
@SpringBootTest(classes = FlinkETLApplication.class)
public class FlinkTest {

    @Test
    public void contextLoads() {
    }

    @Test
    public void restTemplate() {
        RestTemplate restTemplate = new RestTemplate();
        String str = restTemplate.getForObject("http://192.168.0.130:8081/jobs/overview", String.class);
        System.out.println(str);
//        List<FlinkJobOverview> jobs = JSONObject.parseArray(str, FlinkJobOverview.class);
        FlinkJobs jobs = JSONObject.parseObject(str, FlinkJobs.class);
        System.out.println(jobs.toString());
    }



    @Test
    public void getYaml() {
        try {
            Yaml yaml = new Yaml();
            InputStream input = new FileInputStream(System.getProperty("user.dir") + "/src/main/resources/application.yaml");
            Map<String, Object> map = yaml.load(input);
            String username = ((Map<String, Object>)((Map<String, Object>) map.get("spring")).get("datasource")).get("username").toString();
            System.out.println(username);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getUserDir() {
        System.out.println(System.getProperty("user.dir"));
    }

    @Test
    public void example1_flinkTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 读取数据
        String userDir = System.getProperty("user.dir");
        DataStreamSource<String> inputStream = env.readTextFile("C:\\Users\\hcy\\IdeaProjects\\flink-etl\\flink-test\\file\\sensor.txt");

        // 2. 转换成POJO
        DataStream<com.bdilab.demo.examples.pojo.SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3. 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 4. 基于流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        // 5. 调用table API进行转换操作
        Table resultTable = dataTable
                .select($("id"), $("temperature"))
                .where($("id").isEqual("sensor_1"));

        // 6. 执行SQL
        tableEnv.createTemporaryView("sensor", dataTable);
        String sql = "select id, temperature from sensor where id = 'sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }

    @Test
    public void example2_inputFile() throws Exception {
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

        aggTable.printSchema();
        // 打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toRetractStream(aggTable, Row.class).print("agg");
        tableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlagg");

        env.execute();
    }

    @Test
    public void example3_outputFile() throws Exception {
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

    @Test
    public void example4_kafka() throws Exception {
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

    @Test
    public void example4_mysql() throws Exception {
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
                "age int" +
                ") with (" +
                "'connector.type' = 'jdbc'," +
                "'connector.url' = 'jdbc:mysql://localhost:3306/test'," +
                "'connector.table' = 'student'," +
                "'connector.username' = 'root'," +
                "'connector.password' = '123123'" +
                ")";
        tableEnv.executeSql(inputTableSql);
        Table inputTable = tableEnv.from("inputTable");

        // 3. 查询转换
        // 简单转换
        Table resultTable = inputTable.select($("id"), $("name"), $("password"), $("age"))
                .filter($("age").isGreater(13));

        // 聚合统计
//        Table aggTable = inputTable.groupBy($("name"))
//                .select($("name"),
//                        $("age").avg().as("avgAge"));

        String outputSql = "create table outputTable (" +
                "id int," +
                "name varchar," +
                "password varchar," +
                "age int" +
                ") with (" +
                "'connector.type' = 'jdbc'," +
                "'connector.url' = 'jdbc:mysql://localhost:3306/test'," +
                "'connector.table' = 'student_copy'," +
                "'connector.username' = 'root'," +
                "'connector.password' = '123123'" +
                ")";
        tableEnv.executeSql(outputSql);

        resultTable.executeInsert("outputTable");

//        // 聚合操作涉及旧数据的修改更新，文件系统不支持
//        aggTable.executeInsert("outputTable");

        env.execute();
    }

    @Test
    public void example5_proctime() throws Exception {
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
        });

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
                $("timestamp").as("ts"),
                $("temperature").as("temp"),
                $("pt").proctime()
        );

        dataTable.printSchema();

        tableEnv.toAppendStream(dataTable, Row.class).print();

        env.execute();
    }

}
