package com.bdilab.flinketl.utils.kafka;


import com.bdilab.flinketl.utils.WholeVariable;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/**
 * @description:
 * @author: ljw
 * @time: 2021/9/20 18:24
 */

public class RowSerializerOnlyForKafkaProducer implements Serializer<Row> {

    Tuple2<String[], String[]> TypesAndColumns;

    public RowSerializerOnlyForKafkaProducer() {
        TypesAndColumns = new Tuple2<>(
                new String[]{"varchar", "varchar", "varchar"},
                new String[]{"score", "name", "sex"});
    }

    @Override
    public byte[] serialize(String topic, Row row) {

        if (row == null) {
            return null;
        }

        //计算遍历该Row总共需要多少字节
        String[] types = TypesAndColumns.f0;
        String[] columns = TypesAndColumns.f1;
        int byteCount = 0, valueLen = 0;
        for (int i = 0; i < types.length; i++) {

            //for test
            Object value = Objects.requireNonNull(row.getField(columns[i]));

            // 每个属性序列化方式: 属性长度(int，都是4个字节)+属性名称(长度需要计算)+值长度(int，都是4个字节)+值（长度需要计算）
            // 第一个（4）：属性名长度需要的空间, 即int的长度；（int类型反序列化后需要4个字节长度，即32位）
            // 第二个（typeLen）：属性名需要的空间
            // now:
            // 第1个（4）：属性值长度需要的空间
            // 第2个（valueLen）：属性值需要的空间
            switch (types[i]) {
                case WholeVariable.INT:
                    valueLen = 4;
                    break;
                case WholeVariable.FLOAT:
                    valueLen = 4;
                    break;
                case WholeVariable.LONG:
                    valueLen = 8;
                    break;
                case WholeVariable.DOUBLE:
                    valueLen = 8;
                    break;
                case WholeVariable.VARCHAR:
                    //row.getField(fieldName)可能会导致"NullPointerException"
                    //测试后将上面的for-test代码弃用
                    //Object value = Objects.requireNonNull(row.getField(columns[i]));
                    valueLen = value.toString().getBytes(StandardCharsets.UTF_8).length;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported argument type: " + types[i]);
            }
            byteCount += (4 + valueLen);

//            test
//            System.out.println("typeName: " + types[i] +
//                                ", value: " + value.toString() + ", valueLen: " + valueLen);
        }

        // 算出需要的byte[]数组长度后, 分配同样大的byte[]中
        ByteBuffer result = ByteBuffer.allocate(byteCount);
        // 写入result
        for (int i = 0; i < types.length; i++) {
            // 无论什么类型都要先put属性长度和属性名称
//            byte[] typeByte = types[i].getBytes(StandardCharsets.UTF_8);
//            int typeLen = types[i].getBytes(StandardCharsets.UTF_8).length;
//            result.putInt(typeLen);
//            result.put(typeByte);

            Object value = Objects.requireNonNull(row.getField(columns[i]));
            //分别put：value的长度和value的值
            switch (types[i]) {
                case WholeVariable.INT:
                    valueLen = 4;
                    result.putInt(valueLen);
                    result.putInt((int) value);
                    break;
                case WholeVariable.FLOAT:
                    valueLen = 4;
                    result.putInt(valueLen);
                    result.putFloat((float) value);
                    break;
                case WholeVariable.LONG:
                    valueLen = 8;
                    result.putInt(valueLen);
                    result.putLong((long) value);
                    break;
                case WholeVariable.DOUBLE:
                    valueLen = 8;
                    result.putInt(valueLen);
                    result.putDouble((double) value);
                    break;
                case WholeVariable.VARCHAR:
                    byte[] stringByte = ((String) value).getBytes();
                    valueLen = stringByte.length;
                    result.putInt(valueLen);
                    result.put(stringByte);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported argument type: " + types[i]);
            }
        }
        // 返回序列化后的结果
        return result.array();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }
}
