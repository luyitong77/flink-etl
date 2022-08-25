package com.bdilab.flinketl.utils.dataSource;

import com.bdilab.flinketl.entity.ComponentTableInput;
import com.bdilab.flinketl.entity.DatabaseOracle;
import com.bdilab.flinketl.utils.WholeVariable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Map;

/**
 * @description:
 * @author: ljw
 * @time: 2021/8/2 16:38
 */
public class OracleDataSource extends RichSourceFunction<Row> {
    private static final String COMPONENT_TABLE_INPUT = "component_table_input";
    private static final String DATABASE_ORACLE = "database_oracle";

    private int inputId;
    private String yamlPath;
    private static ComponentTableInput tableInput;
    private static DatabaseOracle databaseOracle;

    private PreparedStatement preparedStatement = null;
    private Connection connection = null;

    public OracleDataSource(int sourceInputId, String yamlPath) {
        this.inputId = sourceInputId;
        this.yamlPath = yamlPath;
    }

    /**
     * 从YAML文件中读取的MYSQL连接
     * @return Connection MYSQL连接
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
            //conn就是根据yaml文件建立的MYSQL数据库连接
            //读or写任务的数据源、表名、列名等都要从MySQL的表格中获取
            conn = DriverManager.getConnection(url, username, password);
            return conn;
        } catch (Exception e) {
            throw new Exception("cannot get connection from yaml");
        }
    }

    /**
     * 从component_table_input表中初始化输入任务
     * @param connection 从YAML文件中读取的Oracle连接
     * 直接对static databaseOracle修改
     * @throws Exception "cannot init database oracle"
     */
    private void initDatabaseOracle(Connection connection) throws Exception{
        String rawSQL = "select * from %s where id = %d";
        String sql = String.format(rawSQL, DATABASE_ORACLE, tableInput.getFkDataSourceId());

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
     * 从component_table_input表中初始化输入任务
     * @param connection 从YAML文件中读取的Oracle连接
     * 直接对static tableInput修改
     * @throws Exception cannot init table input
     */
    private void initTableInput(Connection connection) throws Exception{

        //从component_table_input表中读取输入任务
        String rawSQL = "select * from %s where id = %d";
        String sql = String.format(rawSQL, COMPONENT_TABLE_INPUT, inputId);

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        if (resultSet.next()) {
            tableInput = ComponentTableInput.builder()
                    .id(resultSet.getInt("id"))
                    //database_oracle中的key--id
                    .fkDataSourceId(resultSet.getInt("fk_data_source_id"))
                    //oracle数据库中的select任务参数
                    .tableName(resultSet.getString("table_name"))
                    .columns(resultSet.getString("columns"))
                    .columnsType(resultSet.getString("columns_type"))
                    .build();
        } else {
            throw new Exception("cannot init table input");
        }
    }

    private void init() throws Exception{
        Connection connection = getConnectionFromYaml();
        initTableInput(connection);
        initDatabaseOracle(connection);
    }

    /**
     * 初始化方法，读取数据前先初始化Oracle连接，避免多次初始化，有效利用资源。
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
     * @param parameters 参数信息
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        if (tableInput==null || databaseOracle==null) {
            init();
        }

        connection = getConnection();

        String sql = generateSelectSQL();

//        System.out.println(sql);
//        String sql = "select score, name from score;";
        preparedStatement = this.connection.prepareStatement(sql);
    }
    
    /**
     * Source data
     * DataStream 调用一次 run() 方法用来获取数据
     * @param ctx dataStream中的传过来的数据
     * @throws Exception "Illegal input type"
     */
    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        //sourceData的具体信息
        String[] columns = tableInput.getColumns().split(" ");
        String[] types = tableInput.getColumnsType().split(" ");
        //根据拼接成的sql语句获取数据
        ResultSet resultSet = preparedStatement.executeQuery();
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
                    case WholeVariable.LONG:
                        row.setField(columns[i], resultSet.getLong(columns[i]));
                        break;
                    default:
                        throw new Exception("Illegal input type");
                }
            }
            ctx.collect(row);
        }
    }

    @Override
    public void cancel() {

    }

    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception{
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }


    private String generateSelectSQL() {
        String tableName = tableInput.getTableName();
        String[] columns = tableInput.getColumns().split(" ");
        StringBuilder rawSQL = new StringBuilder();
        rawSQL.append("select ");
        for (String column : columns) {
            rawSQL.append("\"").append(column).append("\"").append(",");
        }
        rawSQL.deleteCharAt(rawSQL.length()-1);
        String sql = rawSQL
                .append(" from ")
                .append(tableName)
//                .append(";")
                .toString();
        return sql;
//        String sql = "select score, name from score;";
    }

    private Connection getConnection() {
        String driverClassName = "oracle.jdbc.driver.OracleDriver";
//        String rawUrl = "jdbc:mysql://%s:%d/%s?useUnicode=true&useSSL=false&characterEncoding=utf8";
//        String rawUrl = "jdbc:oracle:thin:@localhost:1521:orcl";
//        String rawUrl = "jdbc:oracle:thin:%s:%d%s%s";
        String rawUrl = databaseOracle.getIsServiceName() == 1
                ? "jdbc:oracle:thin:%s:%d/%s" : "jdbc:oracle:thin:%s:%d:%s";
        String hostname = databaseOracle.getHostname();
        int port = databaseOracle.getPort();
        String databaseName = databaseOracle.getDatabaseName();
        String username = databaseOracle.getUsername();
        String password = databaseOracle.getPassword();

        String url = String.format(rawUrl, hostname, port, databaseName);

        Connection connection = null;
        try {
            Class.forName(driverClassName);
            connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            System.out.println("-----------oracle get connection has exception , msg = "+ e.getMessage());
        }
        return connection;
    }

}
