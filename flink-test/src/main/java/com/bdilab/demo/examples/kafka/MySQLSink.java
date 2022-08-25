package com.bdilab.demo.examples.kafka;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;

import com.bdilab.flinketl.utils.WholeVariable;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

/**
 * @description: 数据库存储类
 * @author: ljw
 * @time: 2021/9/16 19:48
 */
public class MySQLSink extends RichSinkFunction<Row> {

    private static final long serialVersionUID = 1L;
    private Connection connection;
    private PreparedStatement preparedStatement;
    String username = "root";
    String password = "1234";
    String driverName = "com.mysql.jdbc.Driver";
    String url = "jdbc:mysql://localhost:3306/test?serverTimezone=GMT";
    String[] types = {"varchar", "varchar", "varchar"};
    String[] columns = {"id", "name", "age"};
    String tableName = "user_copy1";

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            super.open(parameters);
            Class.forName(driverName);
            connection = DriverManager.getConnection(url, username, password);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        //多少column就要多少个？
        String sql = "insert into "+ tableName + " values(?, ?, ?)";
        preparedStatement = connection.prepareStatement(sql);

        // sql = "insert into tableName values(?, ?, ?)"
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
                default:
                    throw new Exception("Illegal upsert type");
            }
        }
        preparedStatement.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

}
