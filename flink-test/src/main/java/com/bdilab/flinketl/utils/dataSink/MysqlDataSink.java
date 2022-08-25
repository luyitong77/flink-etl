package com.bdilab.flinketl.utils.dataSink;

import com.bdilab.flinketl.entity.ComponentTableUpsert;
import com.bdilab.flinketl.entity.DatabaseMysql;
import com.bdilab.flinketl.utils.WholeVariable;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * @author hcyong
 * @date 2021/7/20
 */
public class MysqlDataSink extends RichSinkFunction<List<Row>> {

    private static final String COMPONENT_TABLE_UPSERT = "component_table_upsert";
    private static final String DATABASE_MYSQL = "database_mysql";

    private int upsertId;
    private String yamlPath;
    private static ComponentTableUpsert tableUpsert;
    private static DatabaseMysql databaseMysql;

    private PreparedStatement preparedStatement;
    private BasicDataSource dataSource;
    private Connection connection;

    public MysqlDataSink(int targetUpsertId, String yamlPath) {
        this.upsertId = targetUpsertId;
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
//        if (applicationContext == null) {
//            applicationContext = new ClassPathXmlApplicationContext("classpath*:applicationContext.xml");
//            tableUpsertService = applicationContext.getBean(ComponentTableUpsertServiceImpl.class);
//            mysqlService = applicationContext.getBean(DatabaseMysqlServiceImpl.class);
//        }
//        tableUpsertService = ApplicationContextUtil.getBean(ComponentTableUpsertServiceImpl.class);
//        mysqlService = ApplicationContextUtil.getBean(DatabaseMysqlServiceImpl.class);

        if (tableUpsert==null || databaseMysql==null) {
            init();
        }

        try {
            super.open(parameters);
            dataSource = new BasicDataSource();
            connection = getConnection(dataSource);
            connection.setAutoCommit(false);
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

//    // 单条写入
//    @Override
//    public void invoke(Row value, Context context) throws Exception {
//        String[] columns = tableUpsert.getColumns().split(" ");
//        String[] types = tableUpsert.getColumnsType().split(" ");
//        // sql = "insert into table values(?, ?, ?)"
//        for (int i=0; i<columns.length; ++i) {
//            switch (types[i]) {
//                case WholeVariable.VARCHAR:
//                    preparedStatement.setString(i+1, String.valueOf(value.getField(columns[i])));
//                    break;
//                case WholeVariable.INT:
//                    preparedStatement.setInt(i+1, Integer.parseInt(String.valueOf(value.getField(columns[i]))));
//                    break;
//                case WholeVariable.FLOAT:
//                    preparedStatement.setFloat(i+1, Float.parseFloat(String.valueOf(value.getField(columns[i]))));
//                    break;
//                case WholeVariable.MYSQL_DATE:
//                    preparedStatement.setDate(i+1, Date.valueOf(String.valueOf(value.getField(columns[i]))));
//                    break;
//                case WholeVariable.MYSQL_TIMESTAMP:
//                    preparedStatement.setTimestamp(i+1, Timestamp.valueOf(String.valueOf(value.getField(columns[i]))));
//                    break;
//                case WholeVariable.MYSQL_DATETIME:
//                    preparedStatement.setTimestamp(i+1, Timestamp.valueOf(String.valueOf(value.getField(columns[i]))));
//                    break;
//                default:
//                    throw new Exception("Illegal upsert type");
//            }
//        }
//        preparedStatement.executeUpdate();
////        preparedStatement.setString(1, String.valueOf(value.getField("name")));
////        preparedStatement.setString(2, String.valueOf(value.getField("password")));
////        preparedStatement.setInt(3, Integer.parseInt(String.valueOf(value.getField("age"))));
////        preparedStatement.setInt(1, Integer.parseInt(String.valueOf(value.getField("score1"))));
////        preparedStatement.setInt(2, Integer.parseInt(String.valueOf(value.getField("score2"))));
////        preparedStatement.setInt(3, Integer.parseInt(String.valueOf(value.getField("score3"))));
////        preparedStatement.setString(4, String.valueOf(value.getField("name")));
////        preparedStatement.executeUpdate();
//    }

    // 批量写入
    @Override
    public void invoke(List<Row> values, Context context) throws Exception {
        int count = 0;
        String[] columns = tableUpsert.getColumns().split(" ");
        String[] types = tableUpsert.getColumnsType().split(" ");
        // 遍历数据集合
        for (Row value : values) {
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
            preparedStatement.addBatch();
            ++count;
//            preparedStatement.setString(1, String.valueOf(row.getField("name")));
//            preparedStatement.setString(2, String.valueOf(row.getField("password")));
//            preparedStatement.setInt(3, Integer.parseInt(String.valueOf(row.getField("age"))));
//            preparedStatement.addBatch();
//            ps.setInt(1, student.getId());
//            ps.setString(2, student.getName());
//            ps.setString(3, student.getPassword());
//            ps.setLong(4, student.getAge());
//            ps.addBatch();
        }
        // 批量后执行
        preparedStatement.executeBatch();
        connection.commit();
        System.out.println("成功了插入了" + count + "行数据");
    }

    private void init() throws Exception{
        Connection conn = getConnectionFromYaml();
        initTableUpsert(conn);
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

    private void initDatabaseMysql(Connection connection) throws Exception{
        String rawSQL = "select * from %s where id = %d";
        String sql = String.format(rawSQL, DATABASE_MYSQL, tableUpsert.getFkDataSourceId());

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
                .append(")")
                .toString();
        return sql;
//        String sql = "insert into score_copy1(score1, score2, score3, name) values (?, ?, ?, ?);";
    }

    private Connection getConnection(BasicDataSource dataSource) {
        String driverClassName = "com.mysql.jdbc.Driver";
        String rawUrl = "jdbc:mysql://%s:%d/%s?useUnicode=true&useSSL=false&characterEncoding=utf8&rewriteBatchedStatements=true&useServerPrepStmts=false";
        String databaseName = databaseMysql.getDatabaseName();
        String hostname = databaseMysql.getHostname();
        int port = databaseMysql.getPort();
        String username = databaseMysql.getUsername();
        String password = databaseMysql.getPassword();

        String url = String.format(rawUrl, hostname, port, databaseName);

        dataSource.setDriverClassName(driverClassName);
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        //设置连接池的一些参数
        dataSource.setInitialSize(2);
        dataSource.setMaxTotal(10);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            System.out.println("创建连接池：" + con);
        } catch (Exception e) {
            System.out.println("-----------mysql get connection / create pool has exception , msg = " + e.getMessage());
        }

        return con;
    }

}
