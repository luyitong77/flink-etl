package com.bdilab.flinketl.flink.utils.OracleUtil;

import com.bdilab.flinketl.entity.ComponentTableInput;
import com.bdilab.flinketl.utils.WholeVariable;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.types.Row;

import java.math.BigDecimal;

/**
 * @description: 将Oracle数据库中的number数据读成的BigDecimal转成int，方便后续数据流的处理
 * @author: ljw
 * @time: 2021/11/2 19:16
 */
public class BigDecimalMapToIntegerFunction extends RichMapFunction<Row, Row> {
    String[] inputColumnsType;
    String[] inputFieldNames;

    public BigDecimalMapToIntegerFunction(ComponentTableInput tableInput) {
        this.inputFieldNames = tableInput.getColumns().split(" ");
        this.inputColumnsType = tableInput.getColumnsType().split(" ");
    }

    @Override
    public Row map(Row value) {
        for (int i = 0; i < inputColumnsType.length; ++i) {
            if (inputColumnsType[i].equals(WholeVariable.ORACLE_NUMBER)) {
                int number = new BigDecimal(value.getField(inputFieldNames[i]).toString()).intValue();
                value.setField(inputFieldNames[i], number);
            }
        }
        return value;
    }
}
