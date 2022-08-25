package com.bdilab.flinketl.utils.dataSink;

import com.bdilab.flinketl.entity.ComponentTableUpsert;
import com.bdilab.flinketl.entity.DatabaseSqlserver;
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

public class SqlServerDataSink extends RichSinkFunction<Row> {
    private static final String COMPONENT_TABLE_UPSERT = "component_table_upsert";
    private static final String DATABASE_SQLSERVER = "database_sqlserver";

    private int upsertId;
    private String yamlPath;
    private static ComponentTableUpsert tableUpsert;
    private static DatabaseSqlserver databaseConfig;

    private PreparedStatement preparedStatement;
    private BasicDataSource dataSource;
    private Connection connection;

    public SqlServerDataSink(int targetUpsertId, String yamlPath){
        this.upsertId=targetUpsertId;
        this.yamlPath = yamlPath;
    }
    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception{
        if (tableUpsert==null || databaseConfig==null) {
            init();
        }
        try {
            super.open(parameters);
            dataSource = new BasicDataSource();
            connection = getConnection(dataSource);
            String sql = generateInsertSQL();
            System.out.println(sql);
//            String sql = "insert into score_copy1(score1, score2, score3, name) values (?, ?, ?, ?);";
            preparedStatement = this.connection.prepareStatement(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }

    // 单条写入
    @Override
    public void invoke(Row value, Context context) throws Exception {
        String[] columns = tableUpsert.getColumns().split(" ");
        String[] types = tableUpsert.getColumnsType().split(" ");
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
                case WholeVariable.MYSQL_DATE:
                    preparedStatement.setDate(i+1, Date.valueOf(String.valueOf(value.getField(columns[i]))));
                    break;
                case WholeVariable.MYSQL_TIMESTAMP:
                    preparedStatement.setTimestamp(i+1, Timestamp.valueOf(String.valueOf(value.getField(columns[i]))));
                    break;
                case WholeVariable.MYSQL_DATETIME:
                    preparedStatement.setTimestamp(i+1, Timestamp.valueOf(String.valueOf(value.getField(columns[i]))));
                    break;
                default:
                    throw new Exception("Illegal upsert type");
            }
        }
        preparedStatement.executeUpdate();
//        preparedStatement.setString(1, String.valueOf(value.getField("name")));
//        preparedStatement.setString(2, String.valueOf(value.getField("password")));
//        preparedStatement.setInt(3, Integer.parseInt(String.valueOf(value.getField("age"))));
//        preparedStatement.setInt(1, Integer.parseInt(String.valueOf(value.getField("score1"))));
//        preparedStatement.setInt(2, Integer.parseInt(String.valueOf(value.getField("score2"))));
//        preparedStatement.setInt(3, Integer.parseInt(String.valueOf(value.getField("score3"))));
//        preparedStatement.setString(4, String.valueOf(value.getField("name")));
//        preparedStatement.executeUpdate();
    }

    // 批量写入
//    @Override
//    public void invoke(List<Row> value, Context context) throws Exception {
//        String[] columns = tableUpsert.getColumns().split(" ");
//        String[] types = tableUpsert.getColumnsType().split(" ");
//        // 遍历数据集合
//        for (Row row : value) {
//            for (int i=0; i<columns.length; ++i) {
//                switch (types[i]) {
//                    case "varchar":
//                        preparedStatement.setString(i+1, String.valueOf(row.getField(columns[i])));
//                        break;
//                    case "int":
//                        preparedStatement.setInt(i+1, Integer.parseInt(String.valueOf(row.getField(columns[i]))));
//                        break;
//                    case "float":
//                        preparedStatement.setFloat(i+1, Float.parseFloat(String.valueOf(row.getField(columns[i]))));
//                        break;
//                    default:
//                        throw new Exception("Illegal upsert type");
//                }
//            }
//            preparedStatement.addBatch();
////            preparedStatement.setString(1, String.valueOf(row.getField("name")));
////            preparedStatement.setString(2, String.valueOf(row.getField("password")));
////            preparedStatement.setInt(3, Integer.parseInt(String.valueOf(row.getField("age"))));
////            preparedStatement.addBatch();
////            ps.setInt(1, student.getId());
////            ps.setString(2, student.getName());
////            ps.setString(3, student.getPassword());
////            ps.setLong(4, student.getAge());
////            ps.addBatch();
//        }
//        // 批量后执行
//        int[] count = preparedStatement.executeBatch();
//        System.out.println("成功了插入了" + count.length + "行数据");
//    }

    private void init() throws Exception{
        Connection conn = getConnectionFromYaml();//这块从yaml中读取连接信息：目前需要mysql的数据源
        initTableUpsert(conn);//读取COMPONENT_TABLE_UPSERT 组件输出表里面的内容
        initDatabaseSqlserver(conn);//从DATABASE_SQLSERVER读取需要进行操作的数据库（sqlServer）ip等相关信息
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

    private void initDatabaseSqlserver(Connection connection) throws Exception{
        String rawSQL = "select * from %s where id = %d";
        String sql = String.format(rawSQL, DATABASE_SQLSERVER, tableUpsert.getFkDataSourceId());

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

    private String generateInsertSQL() {
        String tableName = tableUpsert.getTableName();
        String[] columns = tableUpsert.getColumns().split(" ");
        StringBuilder prefixSQL = new StringBuilder();
        StringBuilder suffixSQL = new StringBuilder();
        prefixSQL.append("insert into ").append(tableName).append("(");
        for (String column : columns) {
            prefixSQL.append(column).append(",");
            suffixSQL.append("?,");
        }
        prefixSQL.deleteCharAt(prefixSQL.length()-1);
        suffixSQL.deleteCharAt(suffixSQL.length()-1);
        String sql = prefixSQL
                .append(") values (")
                .append(suffixSQL)
                .append(");")
                .toString();
        return sql;
//        String sql = "insert into score_copy1(score1, score2, score3, name) values (?, ?, ?, ?);";
    }

    private Connection getConnection(BasicDataSource dataSource) {
        String driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        String rawUrl="jdbc:sqlserver://%s:%d;DatabaseName=%s";
        String databaseName = databaseConfig.getDatabaseName();
        String hostname = databaseConfig.getHostname();
        int port = databaseConfig.getPort();
        String username = databaseConfig.getUsername();
        String password = databaseConfig.getPassword();

        String url = String.format(rawUrl, hostname, port, databaseName);

        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        //设置连接池的一些参数
        dataSource.setInitialSize(2);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            System.out.println("创建连接池：" + con);
        } catch (Exception e) {
            System.out.println("-----------sqlServer get connection / create pool has exception , msg = " + e.getMessage());
        }
        return con;
    }

}
