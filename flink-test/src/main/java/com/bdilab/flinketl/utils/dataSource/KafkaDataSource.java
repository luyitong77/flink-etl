package com.bdilab.flinketl.utils.dataSource;

import com.bdilab.flinketl.entity.DatabaseKafka;
import com.bdilab.flinketl.utils.kafka.RowDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Map;
import java.util.Properties;

/**
 * @description:
 * @author: ljw
 * @time: 2021/9/25 17:22
 */
public class KafkaDataSource{

    private static final String COMPONENT_TABLE_INPUT = "component_table_input";
    private static final String DATABASE_KAFKA = "database_kafka";

    private int inputId;
    private String yamlPath;

    private FlinkKafkaConsumer<Row> dataSource;

    public FlinkKafkaConsumer<Row> getDataSource() {
        return dataSource;
    }

    public KafkaDataSource(int sourceId, String yamlPath) throws Exception {
        this.yamlPath = yamlPath;
        Properties properties = new Properties();
        Tuple2<String[], String[]> tupleTypesColumns = initTableInputInformation(sourceId);
        DatabaseKafka databaseKafka = initKafka(this.inputId, properties);

        dataSource = new FlinkKafkaConsumer<>(
                databaseKafka.getTopicName(),
                new RowDeserializationSchema(tupleTypesColumns),
                properties);
    }

    /**
     * 从YAML文件中读取的MySQL连接
     * @return Connection MySQL连接
     * @throws Exception “cannot get connection from yaml”
     */
    private Connection getConnectionFromYaml() throws Exception{
        try {
            Yaml yaml = new Yaml();
            InputStream input = new FileInputStream(yamlPath);
            Map<String, Object> map = yaml.load(input);
            Map<String, Object> datasource = ((Map<String, Object>)((Map<String, Object>) map.get("spring")).get("datasource"));
            String driver = datasource.get("driver-class-name").toString();
            String url = datasource.get("url").toString();
            String username = datasource.get("username").toString();
            String password = datasource.get("password").toString();

            Connection conn;
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
            return conn;
        } catch (Exception e) {
            throw new Exception("cannot get connection from yaml");
        }
    }

    /**
     * 从component_table_input表中初始化输入任务
     * @param KafkaSourceID id
     * 直接对static tableInput修改
     * @throws Exception cannot get table input information
     */
    private Tuple2<String[], String[]> initTableInputInformation(int KafkaSourceID) throws Exception{

        Connection connection = getConnectionFromYaml();
        //从component_table_input表中读取输入任务
        String rawSQL = "select * from %s where id = %d";
        String sql = String.format(rawSQL, COMPONENT_TABLE_INPUT, KafkaSourceID);

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        if (resultSet.next()) {
            this.inputId = resultSet.getInt("fk_data_source_id");
            return new Tuple2<>(
                    resultSet.getString("columns_type").split(" "),
                    resultSet.getString("columns").split(" "));
        } else {
            throw new Exception("cannot get table input information");
        }
    }

    /**
     * 从YAML文件中读取的MySQL连接
     * 返回Properties让KafkaDataSource/Sink初始化
     * @param KafkaSourceID id
     * @return 配置信息，将topicName以“topicName”包含在其中
     * @throws Exception "cannot init Kafka properties."
     */
    private DatabaseKafka initKafka(int KafkaSourceID, Properties properties) throws Exception{
        Connection connection = getConnectionFromYaml();
        String rawSQL = "select * from %s where id = %d";
        String sql = String.format(rawSQL, DATABASE_KAFKA, KafkaSourceID);

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        if (resultSet.next()) {
            DatabaseKafka databaseKafka = DatabaseKafka.builder()
                    .id(resultSet.getLong("id"))
                    .topicName(resultSet.getString("topic_name"))
                    .bootstrapServers(resultSet.getString("bootstrap_servers"))
                    .sslKeyPassword(resultSet.getString("ssl_key_password"))
                    .sslKeystoreLocation(resultSet.getString("ssl_keystore_location"))
                    .sslKeystorePassword(resultSet.getString("ssl_keystore_password"))
                    .sslTruststoreLocation(resultSet.getString("ssl_truststore_location"))
                    .sslTruststorePassword(resultSet.getString("ssl_truststore_password"))
                    .build();

            properties.setProperty("bootstrap.servers", resultSet.getString("bootstrap_servers"));
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
            properties.put("value.serializer", "com.bdilab.flinketl.utils.kafka.RowDeserializationSchema");
            properties.put("group.id", "test_kafka");
//            properties.put("acks", "1");
//            properties.put("log.flush.interval.ms", 500L);

            if (databaseKafka.getSslKeyPassword() != null) {
                properties.setProperty("ssl.key.password", databaseKafka.getSslKeyPassword());
            }
            if (databaseKafka.getSslKeystoreLocation() != null) {
                properties.setProperty("ssl.keystore.location", databaseKafka.getSslKeystoreLocation());
            }
            if (databaseKafka.getSslKeystorePassword() != null) {
                properties.setProperty("ssl.keystore.password", databaseKafka.getSslKeystorePassword());
            }
            if (databaseKafka.getSslTruststoreLocation() != null) {
                properties.setProperty("ssl.truststore.location", databaseKafka.getSslTruststoreLocation());
            }
            if (databaseKafka.getSslTruststorePassword() != null) {
                properties.setProperty("ssl.truststore.password", databaseKafka.getSslTruststorePassword());
            }

            return databaseKafka;
        } else {
            throw new Exception("cannot init Kafka properties.");
        }
    }



//    /**
//     * 从component_table_input表中初始化输入任务
//     * @param connection 从YAML文件中读取的MySQL连接
//     * 直接对static databaseOracle修改
//     * @throws Exception "cannot init Kafka properties."
//     */
//    private void initDatabaseKafka(Connection connection) throws Exception{
//        String rawSQL = "select * from %s where id = %d";
//        String sql = String.format(rawSQL, DATABASE_KAFKA, tableInput.getFkDataSourceId());
//
//        Statement statement = connection.createStatement();
//        ResultSet resultSet = statement.executeQuery(sql);
//
//        if (resultSet.next()) {
//            databaseKafka = DatabaseKafka.builder()
//                    .id(resultSet.getLong("id"))
//                    .bootstrapServers(resultSet.getString("bootstrap_servers"))
//                    .topicName(resultSet.getString("topic_name"))
//                    .sslKeyPassword(resultSet.getString("ssl_key_password"))
//                    .sslKeystoreLocation(resultSet.getString("ssl_keystore_location"))
//                    .sslKeystorePassword(resultSet.getString("ssl_keystore_password"))
//                    .sslTruststoreLocation(resultSet.getString("ssl_truststore_location"))
//                    .sslTruststorePassword(resultSet.getString("ssl_truststore_password"))
//                    .build();
//            props.setProperty("bootstrap_servers", resultSet.getString("bootstrap_servers"));
//            props.setProperty("ssl_key_password",resultSet.getString("ssl_key_password"));
//            props.setProperty("ssl_keystore_location",resultSet.getString("ssl_keystore_location"));
//            props.setProperty("ssl_keystore_password",resultSet.getString("ssl_key_password"));
//            props.setProperty("ssl_truststore_location",resultSet.getString("ssl_truststore_location"));
//            props.setProperty("ssl_truststore_password",resultSet.getString("ssl_truststore_password"));
//            topicName = resultSet.getString("topic_name");
//        } else {
//            throw new Exception("cannot init Kafka properties.");
//        }
//    }

}
