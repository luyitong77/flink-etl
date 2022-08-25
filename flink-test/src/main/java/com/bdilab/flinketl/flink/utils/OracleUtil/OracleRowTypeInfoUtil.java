package com.bdilab.flinketl.flink.utils.OracleUtil;

import com.bdilab.flinketl.entity.ComponentTableInput;
import com.bdilab.flinketl.utils.WholeVariable;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

/**
 * @description: Oracle转换RowTypeInfo的工具类
 * @author: ljw
 * @time: 2021/11/2 19:22
 */
public class OracleRowTypeInfoUtil {
    /**
     * 要写入Oracle的number中，更新其对应的RowTypeInfo为BasicTypeInfo.INT_TYPE_INFO
     * @param tableInput input信息
     * @return RowTypeInfo
     * @throws Exception Unsupported Oracle output type
     */
    public static RowTypeInfo OracleSinkRowTypeInfo(ComponentTableInput tableInput) throws Exception {
        String[] columnsType = tableInput.getColumnsType().split(" ");;
        String[] fieldNames = tableInput.getColumns().split(" ");
        int len = columnsType.length;
        TypeInformation[] typeInfo = new TypeInformation[len];
        for (int i = 0; i < len; ++i) {
            switch (columnsType[i]) {
                case WholeVariable.VARCHAR:
                case WholeVariable.ORACLE_NVARCHAR2:
                    typeInfo[i] = BasicTypeInfo.STRING_TYPE_INFO;
                    break;
                case WholeVariable.INT:
                case WholeVariable.ORACLE_NUMBER:
                case WholeVariable.FLOAT:
                    typeInfo[i] = BasicTypeInfo.BIG_DEC_TYPE_INFO;
                    break;
                case WholeVariable.DATE:
                case WholeVariable.MYSQL_DATETIME:
                case WholeVariable.MYSQL_TIMESTAMP:
                    typeInfo[i] = SqlTimeTypeInfo.TIMESTAMP;
                    break;
                default:
                    throw new Exception("Unsupported Oracle output type: " + columnsType[i]);
            }
        }
        return new RowTypeInfo(typeInfo, fieldNames);
    }

    /**
     * 将Oracle数据库中读取的BigDecimal转成int后，更新其对应的RowTypeInfo为BasicTypeInfo.DOUBLE_TYPE_INFO
     * @param tableInput input信息
     * @return RowTypeInfo
     * @throws Exception Unsupported Oracle type
     */
    public static RowTypeInfo OracleSourceRowTypeInfo(ComponentTableInput tableInput) throws Exception {
        String[] columnsType = tableInput.getColumnsType().split(" ");;
        String[] fieldNames = tableInput.getColumns().split(" ");
        int len = columnsType.length;
        TypeInformation[] typeInfo = new TypeInformation[len];
        for (int i = 0; i < len; ++i) {
            switch (columnsType[i]) {
                case WholeVariable.ORACLE_NVARCHAR2:
                    typeInfo[i] = BasicTypeInfo.STRING_TYPE_INFO;
                    break;
                case WholeVariable.ORACLE_NUMBER:
                    typeInfo[i] = BasicTypeInfo.INT_TYPE_INFO;
                    break;
                case WholeVariable.DATE:
                    typeInfo[i] = SqlTimeTypeInfo.TIMESTAMP;
                    break;
                default:
                    throw new Exception("Unsupported Oracle type: " + columnsType[i]);
            }
        }
        return new RowTypeInfo(typeInfo, fieldNames);
    }
}
