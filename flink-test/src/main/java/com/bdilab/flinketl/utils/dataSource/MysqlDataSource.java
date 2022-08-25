package com.bdilab.flinketl.utils.dataSource;

import com.bdilab.flinketl.entity.ComponentTableInput;
import com.bdilab.flinketl.entity.DatabaseMysql;
import com.bdilab.flinketl.utils.WholeVariable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Locale;
import java.util.Map;

/**
 * @author hcyong
 * @date 2021/7/19
 */
public class MysqlDataSource extends RichSourceFunction<Row> {

    private static final String COMPONENT_TABLE_INPUT = "component_table_input";
    private static final String DATABASE_MYSQL = "database_mysql";

    private int inputId;
    private String yamlPath;
    private static ComponentTableInput tableInput;
    private static DatabaseMysql databaseMysql;

    private PreparedStatement preparedStatement = null;
    private Connection connection = null;

    public MysqlDataSource(int sourceInputId, String yamlPath) {
        this.inputId = sourceInputId;
        this.yamlPath = yamlPath;

    }

    /**
     * 初始化方法，读取数据前先初始化MySQL连接，避免多次初始化，有效利用资源。
     * @param parameters 参数信息
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
//        if (applicationContext == null) {
//            applicationContext = new ClassPathXmlApplicationContext("classpath*:applicationContext.xml");
//            tableInputService = (ComponentTableInputService) applicationContext.getBean("tableInputService");
//            mysqlService = (DatabaseMysqlService) applicationContext.getBean("mysqlService");
//        }

//        tableInput = tableInputService.getById(inputId);
//        databaseMysql = mysqlService.getById(tableInput.getFkDataSourceId());

        if (tableInput==null || databaseMysql==null) {
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
        ResultSet resultSet = preparedStatement.executeQuery();
        String[] columns, types;
        if (tableInput.getCustomSql() == null) {
            columns = tableInput.getColumns().split(" ");
            types = tableInput.getColumnsType().split(" ");
        } else {
            // 根据 ResultSetMetaData 定义列名和列属性名
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            columns = new String[columnCount];
            types = new String[columnCount];
            for (int i=0; i<columnCount; ++i) {
                columns[i] = metaData.getColumnName(i+1);
                types[i] = metaData.getColumnTypeName(i+1).toLowerCase(Locale.ROOT);
            }
        }
        while (resultSet.next()) {
            Row row = Row.withNames();
            for (int i=0; i<columns.length; ++i) {
                switch (types[i]) {
                    case WholeVariable.VARCHAR:
                        row.setField(columns[i], resultSet.getString(columns[i]));
                        break;
                    case WholeVariable.INT:
                        row.setField(columns[i], resultSet.getInt(columns[i]));
                        break;
                    case WholeVariable.FLOAT:
                        row.setField(columns[i], resultSet.getFloat(columns[i]));
                        break;
                    case WholeVariable.MYSQL_DATE:
                    case WholeVariable.MYSQL_TIMESTAMP:
                    case WholeVariable.MYSQL_TIME:
                    case WholeVariable.MYSQL_DATETIME:
                        row.setField(columns[i],resultSet.getString(columns[i]));//会自动转换
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
        Thread.sleep(4000L);
    }


    private void init() throws Exception{
        Connection conn = getConnectionFromYaml();
        initTableInput(conn);
        initDatabaseMysql(conn);
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
                    .customSql(resultSet.getString("custom_sql"))
                    .build();
        } else {
            throw new Exception("cannot init table input");
        }

    }

    private void initDatabaseMysql(Connection connection) throws Exception{
        String rawSQL = "select * from %s where id = %d";
        String sql = String.format(rawSQL, DATABASE_MYSQL, tableInput.getFkDataSourceId());

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        if (resultSet.next()) {
            databaseMysql = DatabaseMysql.builder()
                    .id(resultSet.getInt("id"))
                    .databaseName(resultSet.getString("database_name"))
                    .hostname(resultSet.getString("hostname"))
                    .port(resultSet.getInt("port"))
                    .username(resultSet.getString("username"))
                    .password(resultSet.getString("password"))
                    .build();
        } else {
            throw new Exception("cannot init database mysql");
        }
    }

    private String generateSelectSQL() {
        String sql;

        if (tableInput.getCustomSql() != null) {
            sql = tableInput.getCustomSql();
        } else {
            String tableName = tableInput.getTableName();
            String[] columns = tableInput.getColumns().split(" ");
            StringBuilder rawSql = new StringBuilder();
            rawSql.append("select ");
            for (String column : columns) {
                rawSql.append(column).append(",");
            }
            rawSql.deleteCharAt(rawSql.length()-1);
            sql = rawSql
                    .append(" from ")
                    .append(tableName)
                    .append(";")
                    .toString();
        }

        return sql;
//        String sql = "select score, name from score;";
    }

    private Connection getConnection() {
        String driverClassName = "com.mysql.jdbc.Driver";
        String rawUrl = "jdbc:mysql://%s:%d/%s?useUnicode=true&useSSL=false&characterEncoding=utf8";
        String databaseName = databaseMysql.getDatabaseName();
        String hostname = databaseMysql.getHostname();
        int port = databaseMysql.getPort();
        String username = databaseMysql.getUsername();
        String password = databaseMysql.getPassword();

        String url = String.format(rawUrl, hostname, port, databaseName);
        Connection con = null;
        try {
            Class.forName(driverClassName);
            con = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}