package com.bdilab.flinketl.utils.dataSource;

import com.bdilab.flinketl.entity.ComponentTableInput;
import com.bdilab.flinketl.entity.DatabaseSqlserver;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Map;

public class SqlServerDataSource extends RichSourceFunction<Row> {

    private static final String COMPONENT_TABLE_INPUT = "component_table_input";
    private static final String DATABASE_SQLSERVER= "database_sqlserver";

    private int inputId;
    private String yamlPath;
    private static ComponentTableInput tableInput;
    private static DatabaseSqlserver databaseConfig;

    private PreparedStatement preparedStatement = null;
    private Connection connection = null;

    public SqlServerDataSource(int sourceInputId, String yamlPath) {
        this.inputId = sourceInputId;
        this.yamlPath = yamlPath;

    }

    /**
     * 初始化方法，读取数据前先初始化Sqlserver连接，避免多次初始化，有效利用资源。
     * @param parameters 参数信息
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
      if (tableInput==null || databaseConfig==null) {
            init();
        }
        connection = getConnection();
        String sql = generateSelectSQL();
        System.out.println(sql);
//        String sql = "select score, name from score;";
        preparedStatement = this.connection.prepareStatement(sql);
    }


    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        String[] columns = tableInput.getColumns().split(" ");
        String[] types = tableInput.getColumnsType().split(" ");
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            Row row = Row.withNames();
            for (int i=0; i<columns.length; ++i) {
                switch (types[i]) {
                    case "varchar":
                        row.setField(columns[i], resultSet.getString(columns[i]));
                        break;
                    case "int":
                        row.setField(columns[i], resultSet.getInt(columns[i]));
                        break;
                    default:
                        throw new Exception("Illegal input type");
                }
            }
//            row.setField("score", resultSet.getString("score"));
//            row.setField("name", resultSet.getString("name"));
            ctx.collect(row);
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception{
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }


    private void init() throws Exception{
        Connection conn = getConnectionFromYaml();
        initTableInput(conn);
        initDatabaseSqlserver(conn);
    }

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

    private void initTableInput(Connection connection) throws Exception{
        String rawSQL = "select * from %s where id = %d";
        String sql = String.format(rawSQL, COMPONENT_TABLE_INPUT, inputId);

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        if (resultSet.next()) {
            tableInput = ComponentTableInput.builder()
                    .id(resultSet.getInt("id"))
                    .fkDataSourceId(resultSet.getInt("fk_data_source_id"))
                    .tableName(resultSet.getString("table_name"))
                    .columns(resultSet.getString("columns"))
                    .columnsType(resultSet.getString("columns_type"))
                    .build();
        } else {
            throw new Exception("cannot init table input");
        }

    }

    private void initDatabaseSqlserver(Connection connection) throws Exception{
        String rawSQL = "select * from %s where id = %d";
        String sql = String.format(rawSQL, DATABASE_SQLSERVER, tableInput.getFkDataSourceId());

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        if (resultSet.next()) {
            databaseConfig = DatabaseSqlserver.builder()
                    .id(resultSet.getInt("id"))
                    .databaseName(resultSet.getString("database_name"))
                    .hostname(resultSet.getString("hostname"))
                    .port(resultSet.getInt("port"))
                    .username(resultSet.getString("username"))
                    .password(resultSet.getString("password"))
                    .build();
        } else {
            throw new Exception("cannot init database sqlserver");
        }

    }

    private String generateSelectSQL() {
        String tableName = tableInput.getTableName();
        String[] columns = tableInput.getColumns().split(" ");
        StringBuilder rawSQL = new StringBuilder();
        rawSQL.append("select ");
        for (String column : columns) {
            rawSQL.append(column).append(",");
        }
        rawSQL.deleteCharAt(rawSQL.length()-1);
        String sql = rawSQL
                .append(" from ")
                .append(tableName)
                .append(";")
                .toString();
        return sql;
//        String sql = "select score, name from score;";
    }

    private Connection getConnection() {
        String driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        String rawUrl="jdbc:sqlserver://%s:%d;DatabaseName=%s";
        String databaseName = databaseConfig.getDatabaseName();
        String hostname = databaseConfig.getHostname();
        int port = databaseConfig.getPort();
        String username = databaseConfig.getUsername();
        String password = databaseConfig.getPassword();

        String url = String.format(rawUrl, hostname, port, databaseName);
        Connection con = null;
        try {
            Class.forName(driverClassName);
            con = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            System.out.println("-----------msqlServer get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
