package com.bdilab.flinketl.flink;

import com.bdilab.flinketl.entity.ComponentFilterColumn;
import com.bdilab.flinketl.entity.ComponentSplitColumn;
import com.bdilab.flinketl.entity.ComponentTableInput;
import com.bdilab.flinketl.flink.utils.FlinkCsvUtil;
import com.bdilab.flinketl.flink.utils.FlinkHiveUtil;
import com.bdilab.flinketl.flink.utils.FlinkJdbcUtil;
import com.bdilab.flinketl.flink.utils.OracleUtil.BigDecimalMapToIntegerFunction;
import com.bdilab.flinketl.flink.utils.OracleUtil.FlinkOracleJdbcUtil;
import com.bdilab.flinketl.flink.utils.OracleUtil.NumberMapToBigDecimalFunction;
import com.bdilab.flinketl.flink.utils.OracleUtil.OracleRowTypeInfoUtil;
import com.bdilab.flinketl.service.*;
import com.bdilab.flinketl.utils.WholeVariable;
import com.bdilab.flinketl.utils.dataSink.KafkaDataSink;
import com.bdilab.flinketl.utils.dataSink.MysqlDataSink;
import com.bdilab.flinketl.utils.dataSink.OracleDataSink;
import com.bdilab.flinketl.utils.dataSink.SqlServerDataSink;
import com.bdilab.flinketl.utils.dataSource.KafkaDataSource;
import com.bdilab.flinketl.utils.dataSource.MysqlDataSource;
import com.bdilab.flinketl.utils.dataSource.OracleDataSource;
import com.bdilab.flinketl.utils.dataSource.SqlServerDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.Serializable;
import java.util.*;

/**
 *
 * 将MySQL中的任务配置信息解析为Flink数据流任务
 *
 * @author hcyong
 * @date 2021/7/17
 */
@Slf4j
@Component
public class FlinkParser implements Serializable {

    @Value("${flink.ip}")
    String ip;
    @Value("${flink.port}")
    int port;
    @Value("${flink.jarPath}")
    String jarPath;
    @Value("${flink.maxParallelism}")
    int maxParallelism;

    @Resource
    DatabaseMysqlService mysqlService;

    @Resource
    ComponentTableInputService tableInputService;
    @Resource
    ComponentTableUpsertService tableUpsertService;
    @Resource
    ComponentFilterColumnService filterColumnService;
    @Resource
    ComponentSplitColumnService splitColumnService;

    @Resource
    FlinkHiveUtil flinkHiveUtil;
    @Resource
    FlinkCsvUtil flinkCsvUtil;
    @Resource
    FlinkJdbcUtil flinkJdbcUtil;
    @Resource
    FlinkOracleJdbcUtil flinkOracleJdbcUtil;

    /**
     * 生成Flink流环境（远程环境/本地）
     * @param isRemote
     * @return
     */
    public StreamExecutionEnvironment generateEnv(boolean isRemote) {
        if (isRemote) {
            return StreamExecutionEnvironment.createRemoteEnvironment(ip, port, jarPath);
        } else {
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
    }

    /**
     * 根据sink数据源类型配置Flink数据源，并异步提交Flink任务
     * @param env
     * @param dataStreamSource
     * @param taskName
     * @param sourceInputId
     * @param targetUpsertId
     * @param targetType
     * @param taskId
     * @param yamlPath
     * @return
     * @throws Exception
     */
    public JobClient executeJob(StreamExecutionEnvironment env, SingleOutputStreamOperator<Row> dataStreamSource, String taskName, int sourceInputId,int targetUpsertId, String targetType, int taskId, String yamlPath) throws Exception {
        switch (targetType) {
            case WholeVariable.MYSQL:
                JdbcOutputFormat jdbcOutput = flinkJdbcUtil.buildJdbcOutputFormat(targetUpsertId, targetType);
                TypeInformation<Row> outputType = dataStreamSource.getTransformation().getOutputType();
                if (outputType instanceof GenericTypeInfo<?>) {
                    dataStreamSource.writeUsingOutputFormat(jdbcOutput);
                } else if (outputType instanceof RowTypeInfo) {
                    RowTypeInfo typeInfo = (RowTypeInfo) dataStreamSource.getType();
                    String[] fieldNames = typeInfo.getFieldNames();
                    SingleOutputStreamOperator<Row> map = dataStreamSource.map(new RichMapFunction<Row, Row>() {
                        @Override
                        public Row map(Row value) throws Exception {
                            String[] names = fieldNames;
                            Row row = Row.withPositions(names.length);
                            for (int i = 0; i < names.length; ++i) {
                                row.setField(i, value.getField(names[i]));
                            }

                            return row;
                        }
                    });
                    map.writeUsingOutputFormat(jdbcOutput);
                }
                return env.executeAsync(taskName);
            case WholeVariable.SQLSERVER:
                SingleOutputStreamOperator outputStreamOperator = dataStreamSource.disableChaining();
                outputStreamOperator.addSink(generateDataSink(targetUpsertId, targetType, yamlPath)).setParallelism(maxParallelism);
                return env.executeAsync(taskName);
            case WholeVariable.ORACLE:
                JdbcOutputFormat OracleJdbcOutput = flinkOracleJdbcUtil.buildJdbcOutputFormat(targetUpsertId);
                dataStreamSource
                        .map(new NumberMapToBigDecimalFunction(tableInputService.getById(sourceInputId)))
                        .returns(OracleRowTypeInfoUtil.OracleSinkRowTypeInfo(tableInputService.getById(sourceInputId)))
                        .writeUsingOutputFormat(OracleJdbcOutput);
                return env.executeAsync(taskName);
            case WholeVariable.KAFKA:
                dataStreamSource.addSink(generateDataSink(targetUpsertId, targetType, yamlPath));
                return env.executeAsync(taskName);
            case WholeVariable.CSV:
                return flinkCsvUtil.generateCsvDataSink(env, dataStreamSource, taskName, targetUpsertId);
            case WholeVariable.HIVE:
                return flinkHiveUtil.generateHiveDataSink(env,dataStreamSource,taskName,targetUpsertId,taskId);
            default:
                throw new Exception("unknown upsert type");
        }
    }

    /**
     * 根据source数据源类型生成Flink数据流
     * @param env
     * @param sourceInputId
     * @param sourceType
     * @param yamlPath
     * @param tableInput
     * @return
     * @throws Exception
     */
    public SingleOutputStreamOperator generateDataSource(StreamExecutionEnvironment env, int sourceInputId, String sourceType, String yamlPath,ComponentTableInput tableInput) throws Exception {

        switch (sourceType) {
            case WholeVariable.MYSQL:
            case WholeVariable.SQLSERVER:
                DataStreamSource<Row> dataStreamSource = env.createInput(flinkJdbcUtil.generateJdbcInputFormat(sourceInputId, sourceType));
                return dataStreamSource;
            case WholeVariable.ORACLE:
                DataStreamSource<Row> OracleDataStreamSource = env.createInput(flinkOracleJdbcUtil.generateJdbcInputFormat(sourceInputId));
                return OracleDataStreamSource
                        .map(new BigDecimalMapToIntegerFunction(tableInputService.getById(sourceInputId)))
                        .returns(OracleRowTypeInfoUtil.OracleSourceRowTypeInfo(tableInputService.getById(sourceInputId)));
            case WholeVariable.KAFKA:
                return env.addSource(generateDataSource(sourceInputId, sourceType, yamlPath));
            case WholeVariable.CSV:
                return flinkCsvUtil.generateCsvDataSource(env, sourceInputId);
            case WholeVariable.HIVE:
                return flinkHiveUtil.generateHiveDataSource(env, tableInput);
            default:
                throw new Exception("Unknown source type");
        }
    }

    /**
     * 根据source数据源类型生成Flink数据流，主要是Kafka
     * @param sourceInputId
     * @param sourceType
     * @param yamlPath
     * @return
     * @throws Exception
     */
    private SourceFunction generateDataSource(int sourceInputId, String sourceType, String yamlPath) throws Exception {

        switch (sourceType) {
            case WholeVariable.MYSQL:
                return new MysqlDataSource(sourceInputId, yamlPath);
            case WholeVariable.SQLSERVER:
                return new SqlServerDataSource(sourceInputId, yamlPath);
            case WholeVariable.ORACLE:
                return new OracleDataSource(sourceInputId, yamlPath);
            case WholeVariable.KAFKA:
                return new KafkaDataSource(sourceInputId, yamlPath).getDataSource();
            default:
                throw new Exception("Unknown source type");
        }
    }

    /**
     * 根据MySQL的过滤组件配置信息来配置Flink数据流
     * @param dataStreamSource
     * @param filterId
     * @return
     * @throws Exception
     */
    public SingleOutputStreamOperator<Row> filterColumn(SingleOutputStreamOperator<Row> dataStreamSource, int filterId) throws Exception {
        ComponentFilterColumn filterColumn = filterColumnService.getById(filterId);

        //获取过滤任务的具体信息
        String columnName = filterColumn.getColumnName();
        String function = filterColumn.getFilterFunction();
        String condition = filterColumn.getFilterCondition();
        int parallelism = filterColumn.getParallelism();

        //根据上述信息构造dataStream的filter
        return dataStreamSource.filter(new RichFilterFunction<Row>() {
            @Override
            public boolean filter(Row value) throws Exception {
                switch (function) {
                    case ">":
                        return Integer.parseInt(String.valueOf(value.getField(columnName))) > Integer.parseInt(condition);
                    case ">=":
                        return Integer.parseInt(String.valueOf(value.getField(columnName))) >= Integer.parseInt(condition);
                    case "<":
                        return Integer.parseInt(String.valueOf(value.getField(columnName))) < Integer.parseInt(condition);
                    case "<=":
                        return Integer.parseInt(String.valueOf(value.getField(columnName))) <= Integer.parseInt(condition);
                    case "=":
                        return Integer.parseInt(String.valueOf(value.getField(columnName))) == Integer.parseInt(condition);
                    default:
                        throw new Exception("unsupported filter function");
                }
            }
        }).setParallelism(parallelism <= maxParallelism ? parallelism : maxParallelism);
    }

    /**
     * 用于Table API中确定返回类型
     * @param inputColumnList
     * @param inputColumnTypeList
     * @param saveColumnNameArr
     * @param saveColumnTypeArr
     * @return
     * @throws Exception
     */
    private RowTypeInfo getRowTypeInfo(List<String> inputColumnList, List<String> inputColumnTypeList, String[] saveColumnNameArr, String[] saveColumnTypeArr) throws Exception {
        List<String> nameList = new ArrayList<>(inputColumnList);
        nameList.addAll(new ArrayList<>(Arrays.asList(saveColumnNameArr)));
        String[] nameArr = nameList.toArray(new String[0]);
        List<String> typeList = new ArrayList<>(inputColumnTypeList);
        typeList.addAll(new ArrayList<>(Arrays.asList(saveColumnTypeArr)));
        String[] typeArr = typeList.toArray(new String[0]);

        int len = nameArr.length;
        String[] fieldName = new String[len];
        TypeInformation[] typeInformation = new TypeInformation[len];

        for (int i = 0; i < len; ++i) {
            fieldName[i] = nameArr[i];
            switch (typeArr[i]) {
                case WholeVariable.VARCHAR:
                    typeInformation[i] = BasicTypeInfo.STRING_TYPE_INFO;
                    break;
                case WholeVariable.INT:
                    typeInformation[i] = BasicTypeInfo.INT_TYPE_INFO;
                    break;
                case WholeVariable.FLOAT:
                    typeInformation[i] = BasicTypeInfo.FLOAT_TYPE_INFO;
                    break;
                default:
                    throw new Exception("unknown type of split/input");
            }
        }
        return new RowTypeInfo(typeInformation, fieldName);
//        TypeInformation[] types = new TypeInformation[4];
//        String[] fieldNames = new String[4];
//        types[0] = BasicTypeInfo.INT_TYPE_INFO;
//        types[1] = BasicTypeInfo.INT_TYPE_INFO;
//        types[2] = BasicTypeInfo.INT_TYPE_INFO;
//        types[3] = BasicTypeInfo.STRING_TYPE_INFO;
//
//        fieldNames[0] = "score1";
//        fieldNames[1] = "score2";
//        fieldNames[2] = "score3";
//        fieldNames[3] = "name";
//
//        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);
//        DataTypes.of(rowTypeInfo);
    }

    /**
     * 根据MySQL的分割组件配置信息来配置Flink数据流
     * @param dataStreamSource
     * @param inputDataId
     * @param splitId
     * @return
     * @throws Exception
     */
    public SingleOutputStreamOperator<Row> splitColumn(SingleOutputStreamOperator<Row> dataStreamSource, int inputDataId, int splitId) throws Exception {
        ComponentTableInput tableInput = tableInputService.getById(inputDataId);
        ComponentSplitColumn splitColumn = splitColumnService.getById(splitId);

        String inputColumns = tableInput.getColumns();
        String inputColumnsType = tableInput.getColumnsType();
        String[] inputColumnsArr = inputColumns.split(" ");
        String[] inputColumnsTypeArr = inputColumnsType.split(" ");
        List<String> inputColumnsList = new LinkedList<>(Arrays.asList(inputColumnsArr));
        List<String> inputColumnsTypeList = new LinkedList<>(Arrays.asList(inputColumnsTypeArr));

        String delimiter = splitColumn.getDelimiter();
        String splitColumnName = splitColumn.getSplitColumnName();
        String saveColumnName = splitColumn.getSaveColumnName();
//        String saveColumnType = splitColumn.getSaveColumnType();
        int parallelism = splitColumn.getParallelism();

        // 删除要分割的列
        int index = inputColumnsList.indexOf(splitColumnName);
        inputColumnsList.remove(index);
        inputColumnsTypeList.remove(index);
        String[] saveColumnNameArr = saveColumnName.split(" ");
//        String[] saveColumnTypeArr = saveColumnType.split(" ");

//        RowTypeInfo rowTypeInfo = getRowTypeInfo(inputColumnsList, inputColumnsTypeList, saveColumnNameArr, saveColumnTypeArr);

        System.out.println(inputColumnsList);
        System.out.println(Arrays.toString(saveColumnNameArr));

        return dataStreamSource
                .map(new RichMapFunction<Row, Row>() {
                    @Override
                    public Row map(Row value) throws Exception {
                        String[] splitColumnValue = String.valueOf(value.getField(splitColumnName)).split(delimiter);
                        Row row = Row.withNames();
                        // 重新创建Row
                        for (String columnName : inputColumnsList) {
                            row.setField(columnName, value.getField(columnName));
                        }
                        for (int i = 0; i < saveColumnNameArr.length; ++i) {
                            row.setField(saveColumnNameArr[i], Integer.parseInt(String.valueOf(splitColumnValue[i])));
                        }
//                        System.out.println(row.getField("score1"));
                        return row;
                    }
                }).setParallelism(parallelism <= maxParallelism ? parallelism : maxParallelism);
    }

    /**
     * 根据sink数据源类型配置Flink数据流
     * @param targetUpsertId
     * @param targetType
     * @param yamlPath
     * @return
     * @throws Exception
     */
    public SinkFunction generateDataSink(int targetUpsertId, String targetType, String yamlPath) throws Exception {
        switch (targetType) {
            case WholeVariable.MYSQL:
                return new MysqlDataSink(targetUpsertId, yamlPath);
            case WholeVariable.SQLSERVER:
                return new SqlServerDataSink(targetUpsertId, yamlPath);
            case WholeVariable.ORACLE:
                return new OracleDataSink(targetUpsertId, yamlPath);
            case WholeVariable.KAFKA:
                return new KafkaDataSink(targetUpsertId, yamlPath).getDataSink();
            default:
                throw new Exception("Illegal target type to sink");
        }
    }

    //    public String generateSQL(int dataId, String dataType, String inputOrOutput) throws Exception {
//        switch (dataType) {
//            case WholeVariable.MYSQL:
//                return generateMysql(dataId, inputOrOutput);
//            default:
//                throw new Exception("error datatype");
//        }
//    }

//    public StreamTableEnvironment createInputEnv(String inputTableSQL) {
////        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(ip, port, jarPath);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        tableEnv.executeSql(inputTableSQL);
//        return tableEnv;
////        return env;
//    }
//
//    public StreamTableEnvironment createOutputEnv(StreamTableEnvironment tableEnv, String outputTableSQL) {
//        tableEnv.executeSql(outputTableSQL);
//        return tableEnv;
//    }

//    public Table filterColumn(Table inputTable, int dataId, int filterId) throws Exception {
//        ComponentTableInput tableInput = tableInputService.getById(dataId);
//        ComponentFilterColumn filterColumn = filterColumnService.getById(filterId);
//        String[] columns = tableInput.getColumns().split(" ");
////        String columns = tableInput.getColumns().replaceAll(" ",",");
////        inputTable.select(columns);
//        List<Expression> expressionList = new LinkedList<>();
//        for (String column : columns) {
//            expressionList.add($(column));
//        }
//
//        String columnName = filterColumn.getColumnName();
//        String function = filterColumn.getFilterFunction();
//        String condition = filterColumn.getFilterCondition();
//        inputTable.select(expressionList.toArray(new Expression[0]));
//
//        Expression filter;
//        switch (function) {
//            case ">":
//                filter = $(columnName).isGreater(Integer.valueOf(condition));
//                break;
//            case "<":
//                filter = $(columnName).isLess(Integer.valueOf(condition));
//                break;
//            case "=":
//                filter = $(columnName).isEqual(Integer.valueOf(condition));
//                break;
//            default:
//                throw new Exception("unsupported filter function");
//        }
//
//        return inputTable.filter(filter);
//    }

//    private String generateMysql(int dataId, String inputOrOutput) throws Exception {
//        switch (inputOrOutput) {
//            case "input":
//                return generateInputMysql(dataId);
//            case "output":
//                return generateUpsertMysql(dataId);
//            default:
//                throw new Exception("error input/output");
//        }
//    }

//    private String generateInputMysql(int dataId) {
//        ComponentTableInput tableInput = tableInputService.getById(dataId);
//        DatabaseMysql mysql = mysqlService.getById(tableInput.getFkDataSourceId());
//
////        String createTableSQL = "create table inputTable (" +
////                "id int," +
////                "name varchar," +
////                "password varchar," +
////                "age int," +
////                "primary key(id) not enforced" +
////                ") with (" +
////                "'connector.type' = 'jdbc'," +
////                "'connector.url' = 'jdbc:mysql://192.168.0.239:3306/test'," +
////                "'connector.table' = 'student'," +
////                "'connector.username' = 'root'," +
////                "'connector.password' = '123123'" +
////                ")";
//
//        String[] columns = tableInput.getColumns().split(" ");
//        String[] columnsType = tableInput.getColumnsType().split(" ");
//        int columnsLen = columns.length;
//
//        if (columns.length != columnsType.length) {
//            return "input columns and type don't match";
//        }
//
////        StringBuilder[] sbs = new StringBuilder[columnsLen+1];
////        sbs[0].append("create table inputTable (");
////        for (int i=0; i<columnsLen; ++i) {
////            sbs[i+1].append(columns[i]).append(" ").append(columnsType[i]).append(",");
////        }
////        sbs[columnsLen].deleteCharAt(sbs[columnsLen].length()-1);
//
//        StringBuilder sb = new StringBuilder();
//        sb.append("create table inputTable (");
//        for (int i = 0; i < columnsLen; ++i) {
//            sb.append(columns[i]).append(" ").append(columnsType[i]).append(",");
//        }
//        sb.deleteCharAt(sb.length() - 1);
//
//        String connectorSQL = ") with (" +
//                "'connector.type' = 'jdbc'," +
//                "'connector.url' = 'jdbc:mysql://%s:%d/%s?useSSL=false'," +
//                "'connector.table' = '%s'," +
//                "'connector.username' = '%s'," +
//                "'connector.password' = '%s'" +
//                ")";
//        String connector = String.format(connectorSQL,
//                mysql.getHostname(),
//                mysql.getPort(),
//                mysql.getDatabaseName(),
//                tableInput.getTableName(),
//                mysql.getUsername(),
//                mysql.getPassword());
//        sb.append(connector);
//        return sb.toString();
//    }
//
//    private String generateUpsertMysql(int dataId) {
//        ComponentTableUpsert tableUpsert = tableUpsertService.getById(dataId);
//        DatabaseMysql mysql = mysqlService.getById(tableUpsert.getFkDataSourceId());
//
//        String[] columns = tableUpsert.getColumns().split(" ");
//        String[] columnsType = tableUpsert.getColumnsType().split(" ");
//        int columnsLen = columns.length;
//
//        if (columns.length != columnsType.length) {
//            return "upsert columns and type don't match";
//        }
//
//        StringBuilder sb = new StringBuilder();
//        sb.append("create table outputTable (");
//        for (int i = 0; i < columnsLen; ++i) {
//            sb.append(columns[i]).append(" ").append(columnsType[i]).append(",");
//        }
//        sb.deleteCharAt(sb.length() - 1);
//
//        String connectorSQL = ") with (" +
//                "'connector.type' = 'jdbc'," +
//                "'connector.url' = 'jdbc:mysql://%s:%d/%s?useSSL=false'," +
//                "'connector.table' = '%s'," +
//                "'connector.username' = '%s'," +
//                "'connector.password' = '%s'" +
//                ")";
//        String connector = String.format(connectorSQL,
//                mysql.getHostname(),
//                mysql.getPort(),
//                mysql.getDatabaseName(),
//                tableUpsert.getTableName(),
//                mysql.getUsername(),
//                mysql.getPassword());
//        sb.append(connector);
//        return sb.toString();
//    }


}
