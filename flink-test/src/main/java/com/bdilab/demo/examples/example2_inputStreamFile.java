package com.bdilab.demo.examples;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class example2_inputStreamFile {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 表的创建：连接外部系统，读取数据
        // 2.1 读取文件
        String filePath = "hdfs://192.168.0.130:9000/file/csvInput/6046e574-179f-4e0c-a287-890fb8dd676e_121.csv";
        DataStream<String> dataStream = env.readTextFile(filePath);
        SingleOutputStreamOperator<List<String>> map = dataStream.map(new RichMapFunction<String, List<String>>() {
            @Override
            public List<String> map(String s) throws Exception {
                String[] data = s.split(",");
                return new LinkedList<>(Arrays.asList(data));
            }
        });
        dataStream.print("hdfs");

        map.print("csv");

        env.execute();
    }

}
