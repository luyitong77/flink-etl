package com.bdilab.demo.examples.kafka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @description:
 * @author: ljw
 * @time: 2021/9/19 21:28
 */
public class MySQLSource extends RichSourceFunction<Row> {

    private Connection connection;
    private PreparedStatement preparedStatement;
    String username = "root";
    String password = "1234";
    String driverName = "com.mysql.jdbc.Driver";
    String url = "jdbc:mysql://localhost:3306/test?serverTimezone=GMT";
    String sql = "select * from user";

    /**
     * 初始化方法
     * open() 方法中建立连接，这样不用每次 run 的时候都要建立连接和释放连接。
     * @param parameters 参数信息
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            super.open(parameters);
            Class.forName(driverName);
            connection = DriverManager.getConnection(url, username, password);
            preparedStatement = this.connection.prepareStatement(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.toString());
            Row row = Row.withNames();
//            row.setField(0, resultSet.getString(1));
            row.setField("id", resultSet.getString("id"));
            row.setField("name", resultSet.getString("name"));
            row.setField("age", resultSet.getString("age"));
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
}
