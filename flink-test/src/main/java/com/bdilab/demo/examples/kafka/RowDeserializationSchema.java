package com.bdilab.demo.examples.kafka;

import com.bdilab.flinketl.utils.kafka.SerializationUtil;
import com.bdilab.flinketl.utils.WholeVariable;
import lombok.AllArgsConstructor;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * @description:
 * @author: ljw
 * @time: 2021/9/24 16:05
 */
@AllArgsConstructor
public class RowDeserializationSchema implements DeserializationSchema<Row> {

    Tuple2<String[], String[]> tuple;

    @Override
    public Row deserialize(byte[] message) {
        Row row = Row.withNames();
        int len = message.length, i = 0, j = 0, valueLen = 0;
        String[] types = tuple.f0;
        String[] columns = tuple.f1;
        String type;
        Object value;
        while (i < len && j < types.length) {
            valueLen = SerializationUtil.bytesToInt(Arrays.copyOfRange(message, i, i + 4));
            i += 4;
            type = types[j];
            switch (type) {
                case WholeVariable.INT:
                    // valueLen = 4;
                    value = SerializationUtil.bytesToInt(Arrays.copyOfRange(message, i, i+valueLen));
                    break;
                case WholeVariable.FLOAT:
                    // valueLen = 4;
                    value = SerializationUtil.bytesToFloat(Arrays.copyOfRange(message, i, i+valueLen));
                    break;
                case WholeVariable.LONG:
                    // valueLen = 8;
                    value = SerializationUtil.bytesToLong(Arrays.copyOfRange(message, i, i+valueLen));
                    break;
                case WholeVariable.DOUBLE:
                    // valueLen = 8;
                    value = SerializationUtil.bytesToDouble(Arrays.copyOfRange(message, i, i+valueLen));
                    break;
                case WholeVariable.VARCHAR:
//                    valueLen = value.toString().getBytes(StandardCharsets.UTF_8).length;
                    value = SerializationUtil.bytesToString(Arrays.copyOfRange(message, i, i+valueLen));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported argument type: " + type);
            }
            row.setField(columns[j], value);
            i += valueLen;
            j++;
        }
        return row;
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        String[] types = tuple.f0;
        TypeInformation[] typeInformation = new TypeInformation[types.length];
        for (int i = 0; i < types.length; i++) {
            switch (types[i]) {
                case WholeVariable.INT:
                    typeInformation[i] = BasicTypeInfo.INT_TYPE_INFO;
                    break;
                case WholeVariable.FLOAT:
                    typeInformation[i] = BasicTypeInfo.FLOAT_TYPE_INFO;
                    break;
                case WholeVariable.LONG:
                    typeInformation[i] = BasicTypeInfo.LONG_TYPE_INFO;
                    break;
                case WholeVariable.DOUBLE:
                    typeInformation[i] = BasicTypeInfo.DOUBLE_TYPE_INFO;
                    break;
                case WholeVariable.VARCHAR:
                    typeInformation[i] = BasicTypeInfo.STRING_TYPE_INFO;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported argument type: " + types[i]);
            }
        }
        return new RowTypeInfo(typeInformation, tuple.f1);
//        return new RowTypeInfo(typeInformation);
    }
}
