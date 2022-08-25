package com.bdilab.demo;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 *  在 Java DataStream API 中的 DataStream 支持的表上使用 SQL
 *
 *  1. 将两个无界的数据流转化为表
 *  2. 注册一个表为视图
 *  3. 在注册与未注册的表上使用流式SQL查询
 *  4. 将 table 转换为数据流
 */
public class StreamSQLExample {
    public static void main(String[] args) throws Exception {
        // set up the Java DataStream API
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the Java Table API
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final DataStream<Order> orderA =
                env.fromCollection(
                        Arrays.asList(
                                new Order(1L, "beer", 3),
                                new Order(1L, "diaper", 4),
                                new Order(3L, "rubber", 2)));

        final DataStream<Order> orderB =
                env.fromCollection(
                        Arrays.asList(
                                new Order(2L, "pen", 3),
                                new Order(2L, "rubber", 3),
                                new Order(4L, "beer", 1)));

        // 将DataStream转化为table对象
        final Table tableA = tableEnv.fromDataStream(orderA);

        // 将第二个DataStream转化为 Table ，并且注册视图
        // 通过"TableB"可访问
        tableEnv.createTemporaryView("TableB", orderB);

        // 执行 union 操作
        final Table result =
                tableEnv.sqlQuery(
                        "SELECT * FROM "
                                + tableA
                                + " WHERE amount > 2 UNION ALL "
                                + "SELECT * FROM TableB WHERE amount < 2");

        // 再由table转换为DataStream
        System.out.println("\n\n\n\n\n");
        tableEnv.toDataStream(result, Order.class).print();
        System.out.println("\n\n\n\n\n");

        // 必须用 execute() 提交任务
        env.execute();

    }

    /**
     *  一个 POJO
     */
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        // for POJO detection in DataStream API
        public Order() {}

        // for structured type detection in Table API
        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{"
                    + "user="
                    + user
                    + ", product='"
                    + product
                    + '\''
                    + ", amount="
                    + amount
                    + '}';
        }
    }
}
