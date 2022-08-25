package com.bdilab.demo.examples;

import com.bdilab.flinketl.utils.WholeVariable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Types;

/**
 * @description:
 * @author: ljw
 * @time: 2021/10/30 19:54
 */
public class example6_JDBCoutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> ds = env.fromElements("101,100","201,100");
//        ds.map(new MapFunction<String, Row>() {
//            @Override
//            public Row map(String s) throws Exception {
//                String[] ss = s.split(",");
//                Row row = new Row(2);
//                row.setField(0,Integer.parseInt(ss[0]));
//                row.setField(1,String.valueOf(ss[1]));
//                return row;
//            }
//        }).writeUsingOutputFormat(
//                JdbcOutputFormat.buildJdbcOutputFormat()
//                        .setDBUrl("jdbc:oracle:thin:@localhost:1521:orcl")
//                        .setDrivername("oracle.jdbc.driver.OracleDriver")
//                        .setUsername("sys as sysdba")
//                        .setPassword("1234")
//                        .setSqlTypes(new int[]{Types.INTEGER,Types.VARCHAR})
//                        .setQuery("insert into STUDENT(\"id\", \"name\") values(?,?)")
//                        .finish())
//        ;
//        env.execute();

//        TypeInformation[] typeInfo = new TypeInformation[]{BasicTypeInfo.BIG_DEC_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP};
//        String[] filedName = new String[]{"id", "name", "birthday"};
//
//        SingleOutputStreamOperator dataSource = env.createInput(JdbcInputFormat.buildJdbcInputFormat()
//                .setDBUrl("jdbc:oracle:thin:@localhost:1521:orcl")
//                .setDrivername("oracle.jdbc.driver.OracleDriver")
//                .setUsername("sys as sysdba")
//                .setPassword("1234")
//                .setQuery("select \"id\", \"name\", \"birthday\" from STUDENT_COPY1")
//                .setRowTypeInfo(new RowTypeInfo(typeInfo, filedName))
//                .finish())
//                .map(new RichMapFunction<Row, Row>() {
//                    @Override
//                    public Row map(Row value) {
//                        int x = new BigDecimal(value.getField("id").toString()).intValue();
//                        value.setField("id", x);
//                        return value;
//                    }
//                })
//                .returns(new RowTypeInfo(
//                        new TypeInformation[]{
//                                BasicTypeInfo.INT_TYPE_INFO,
//                                BasicTypeInfo.STRING_TYPE_INFO,
//                                SqlTimeTypeInfo.TIMESTAMP},
//                        filedName));
//
////
//
//        dataSource.writeUsingOutputFormat(
//                JdbcOutputFormat.buildJdbcOutputFormat()
//                        .setDBUrl("jdbc:mysql://localhost:3306/test")
//                        .setDrivername(WholeVariable.MYSQL_DRIVER_NAME)
//                        .setUsername("root")
//                        .setPassword("1234")
//                        .setSqlTypes(new int[]{Types.INTEGER, Types.VARCHAR, Types.TIMESTAMP_WITH_TIMEZONE})
//                        .setQuery("insert into student(id, name, birthday) values(?,?,?)")
//                        .finish())
//        ;
//        env.execute();

        TypeInformation[] typeInfo = new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP};
        String[] filedName = new String[]{"id", "name", "birthday"};
        SingleOutputStreamOperator dataSource = env.createInput(JdbcInputFormat.buildJdbcInputFormat()
                .setDBUrl("jdbc:mysql://localhost:3306/test")
                .setDrivername(WholeVariable.MYSQL_DRIVER_NAME)
                .setUsername("root")
                .setPassword("1234")
                .setQuery("select id, name, birthday from student")
                .setRowTypeInfo(new RowTypeInfo(typeInfo, filedName))
                .finish())
                .map(new RichMapFunction<Row, Row>() {
                    @Override
                    public Row map(Row value) {
                        BigDecimal x = new BigDecimal(value.getField("id").toString());
                        value.setField("id", x);
                        return value;
                    }
                })
                .returns(new RowTypeInfo(
                        new TypeInformation[]{
                                BasicTypeInfo.BIG_DEC_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                SqlTimeTypeInfo.TIMESTAMP},
                        filedName));
        ;

        dataSource.writeUsingOutputFormat(
                JdbcOutputFormat.buildJdbcOutputFormat()
                        .setDBUrl("jdbc:oracle:thin:@localhost:1521:orcl")
                        .setDrivername("oracle.jdbc.driver.OracleDriver")
                        .setUsername("sys as sysdba")
                        .setPassword("1234")
                        .setSqlTypes(new int[]{Types.DECIMAL, Types.VARCHAR, Types.TIMESTAMP_WITH_TIMEZONE})
                        .setQuery("insert into STUDENT_COPY1(\"id\", \"name\", \"birthday\") values(?,?,?)")
                        .finish())
        ;
        env.execute();

    }
}
