package com.bdilab.flinketl.utils.dataSink;

import com.bdilab.flinketl.entity.ComponentTableUpsert;
import com.bdilab.flinketl.entity.DatabaseOracle;
import com.bdilab.flinketl.utils.WholeVariable;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Map;

/**
 * @description:
 * @author: ljw
 * @time: 2021/8/5 10:39
 */
public class OracleDataSink extends RichSinkFunction<Row> {
    private static final String COMPONENT_TABLE_UPSERT = "component_table_upsert";
    private static final String DATABASE_ORACLE = "database_oracle";

    private int upsertId;
    private String yamlPath;

    private static ComponentTableUpsert tableUpsert;
    private static DatabaseOracle databaseOracle;

    private PreparedStatement preparedStatement = null;
    private BasicDataSource dataSource;
    private Connection connection;

    public OracleDataSink(int sourceInputId, String yamlPath) {
        this.upsertId = sourceInputId;
        this.yamlPath = yamlPath;
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
     * 从component_table_upsert表中初始化输入任务
     * @param connection 从YAML文件中读取的MySQL连接
     * 直接对static tableUpsert
     * @throws Exception cannot init table input
     */
    private void initTableUpsert(Connection connection) throws Exception{
        String rawSQL = "select * from %s where id = %d";
        String sql = String.format(rawSQL, COMPONENT_TABLE_UPSERT, upsertId);

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        if (resultSet.next()) {
            tableUpsert = ComponentTableUpsert.builder()
                    .id(resultSet.getInt("id"))
                    .fkDataSourceId(resultSet.getInt("fk_data_source_id"))
                    .tableName(resultSet.getString("table_name"))
                    .columns(resultSet.getString("columns"))
                    .columnsType(resultSet.getString("columns_type"))
                    .build();
        } else {
            throw new Exception("cannot init table upsert");
        }
    }


    private Connection getConnection(BasicDataSource basicDataSource) {
        String driverClassName = "oracle.jdbc.driver.OracleDriver";
        String rawUrl = databaseOracle.getIsServiceName() == 1 ? "jdbc:oracle:thin:%s:%d/%s" : "jdbc:oracle:thin:%s:%d:%s";
//        String rawUrl = "jdbc:oracle:thin:%s:%d%s%s";
        String hostname = databaseOracle.getHostname();
        int port = databaseOracle.getPort();
        String databaseName = databaseOracle.getDatabaseName();
        String url = String.format(rawUrl, hostname, port, databaseName);

        String username = databaseOracle.getUsername();
        String password = databaseOracle.getPassword();

        basicDataSource.setDriverClassName(driverClassName);
        //注意，替换成自己本地的 Oracle 数据库地址和用户名、密码
        basicDataSource.setUrl(url);
        basicDataSource.setUsername(username);
        basicDataSource.setPassword(password);
        //设置连接池的一些参数
        basicDataSource.setInitialSize(2);
        basicDataSource.setMaxTotal(50);
        basicDataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = basicDataSource.getConnection();
            System.out.println("创建连接池：" + con);
        } catch (Exception e) {
            System.out.println("-----------Oracle get connection / create pool has exception , msg = " + e.getMessage());
        }
        return con;
    }

    /**
     * 从component_table_input表中初始化输入任务
     * @param connection 从YAML文件中读取的MySQL连接
     * 直接对static databaseOracle修改
     * @throws Exception "cannot init database oracle"
     */
    private void initDatabaseOracle(Connection connection) throws Exception{
        String rawSQL = "select * from %s where id = %d";
        String sql = String.format(rawSQL, DATABASE_ORACLE, tableUpsert.getFkDataSourceId());

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        if (resultSet.next()) {
            databaseOracle = DatabaseOracle.builder()
                    .id(resultSet.getLong("id"))
                    .databaseName(resultSet.getString("database_name"))
                    .hostname(resultSet.getString("hostname"))
                    .port(resultSet.getInt("port"))
                    .isServiceName(resultSet.getInt("is_service_name"))
                    .username(resultSet.getString("username"))
                    .password(resultSet.getString("password"))
                    .build();
        } else {
            throw new Exception("cannot init database oracle");
        }
    }

    /**
     * 根据component_table_upsert中的数据拼接成insert的SQL语句
     */
    private String generateInsertSQL() {
        String tableName = tableUpsert.getTableName();
        String[] columns = tableUpsert.getColumns().split(" ");
        StringBuilder prefixSQL = new StringBuilder();
        StringBuilder suffixSQL = new StringBuilder();
        prefixSQL.append("insert into ").append(tableName).append("(\"id\", ");
        for (String column : columns) {
            prefixSQL.append("\"").append(column).append("\"").append(",");
            suffixSQL.append("?,");
        }
        prefixSQL.deleteCharAt(prefixSQL.length()-1);
        suffixSQL.deleteCharAt(suffixSQL.length()-1);
        String sql = prefixSQL
                .append(") values (77,")
                .append(suffixSQL)
                .append(")")
                .toString();
        return sql;
//        String sql = "insert into score_copy1(score1, score2, score3, name) values (?, ?, ?, ?);";
    }

    private void init() throws Exception{
        Connection conn = getConnectionFromYaml();
        initTableUpsert(conn);
        initDatabaseOracle(conn);
    }

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception{

        if (tableUpsert==null || databaseOracle==null) {
            init();
        }

        try {
            super.open(parameters);
            dataSource = new BasicDataSource();
            connection = getConnection(dataSource);
            String sql = generateInsertSQL();
//            String sql = "insert into score_copy1(score1, score2, score3, name) values (?, ?, ?, ?);";
            preparedStatement = this.connection.prepareStatement(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 单条写入
    @Override
    public void invoke(Row value, Context context) throws Exception {
        String[] columns = tableUpsert.getColumns().split(" ");
        String[] types = tableUpsert.getColumnsType().split(" ");
//        String tableName = tableUpsert.getTableName();
//        String[] columns = tableUpsert.getColumns().split(" ");
//        String sql = "insert into tableName(columns...) values (?...);";
//        String.valueOf( value.getField( columns[i] ) )
        for (int i=0; i<columns.length; ++i) {
            switch (types[i]) {
                case WholeVariable.VARCHAR:
                    preparedStatement.setString(i+1, String.valueOf(value.getField(columns[i])));
                    break;
                case WholeVariable.INT:
                    preparedStatement.setInt(i+1, Integer.parseInt(String.valueOf(value.getField(columns[i]))));
                    break;
                case WholeVariable.FLOAT:
                    preparedStatement.setFloat(i+1, Float.parseFloat(String.valueOf(value.getField(columns[i]))));
                    break;
                case WholeVariable.LONG:
                    preparedStatement.setLong(i+1, Long.parseLong(String.valueOf(value.getField(columns[i]))));
                default:
                    throw new Exception("Illegal upsert type");
            }
        }

        preparedStatement.executeUpdate();
    }
}
