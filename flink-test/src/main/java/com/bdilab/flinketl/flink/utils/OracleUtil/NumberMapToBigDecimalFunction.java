package com.bdilab.flinketl.flink.utils.OracleUtil;

import com.bdilab.flinketl.entity.ComponentTableInput;
import com.bdilab.flinketl.utils.WholeVariable;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.types.Row;

import java.math.BigDecimal;

/**
 * @description: 把要写入Oracle的数据中将被转化为Number的所有数据转化为BigDecimal
 * @author: ljw
 * @time: 2021/11/2 19:20
 */
public class NumberMapToBigDecimalFunction  extends RichMapFunction<Row, Row> {
    String[] inputColumnsType;
    String[] inputFieldNames;

    public NumberMapToBigDecimalFunction(ComponentTableInput tableInput) {
        this.inputFieldNames = tableInput.getColumns().split(" ");
        this.inputColumnsType = tableInput.getColumnsType().split(" ");
    }

    @Override
    public Row map(Row value) {
        for (int i = 0; i < inputColumnsType.length; ++i) {
            //只要输入源的该列转入Oracle是number
            if (WholeVariable.NUMBERS.contains(inputColumnsType[i])) {
                BigDecimal number = new BigDecimal(value.getField(inputFieldNames[i]).toString());
                value.setField(inputFieldNames[i], number);
            }
        }
        return value;
    }
}
