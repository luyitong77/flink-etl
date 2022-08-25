package com.bdilab.flinketl.flink.utils;

import com.bdilab.flinketl.entity.ComponentTableInput;
import com.bdilab.flinketl.entity.ComponentTableUpsert;
import com.bdilab.flinketl.entity.DatabaseHive;
import com.bdilab.flinketl.entity.SysCommonTask;
import com.bdilab.flinketl.mapper.SysCommonTaskMapper;
import com.bdilab.flinketl.service.ComponentTableInputService;
import com.bdilab.flinketl.service.ComponentTableUpsertService;
import com.bdilab.flinketl.service.DatabaseHiveService;
import com.bdilab.flinketl.utils.WholeVariable;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

@Slf4j
@Component
public class FlinkHiveUtil {

    @Value("${flink.maxParallelism}")
    int maxParallelism;
    @Value("${hive.conf.dir}")
    String hiveConfDir;
    //不能写死，而且要保证是多个任务都不同,先弄随机
    @Resource
    SysCommonTaskMapper commonTaskMapper;
    @Resource
    DatabaseHiveService databaseHiveService;
    @Resource
    ComponentTableInputService tableInputService;
    @Resource
    ComponentTableUpsertService tableUpsertService;

    public SingleOutputStreamOperator<Row> generateHiveDataSource(StreamExecutionEnvironment env, ComponentTableInput tableInput) throws Exception {
        DatabaseHive hiveConf = databaseHiveService.getHiveInfo(tableInput.getFkDataSourceId());
        StreamTableEnvironment tableEnv=generateHiveTableEnv(env,hiveConf);
        return generateHiveTableSource(tableEnv,tableInput.getTableName(),tableInput);
    }
    private StreamTableEnvironment generateHiveTableEnv(StreamExecutionEnvironment env, DatabaseHive hiveConf){
        String catalogName="myhive_"+ UUID.randomUUID();
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv= StreamTableEnvironment.create(env,envSettings);
//        StreamTableEnvironment tableEnv=StreamTableEnvironment.create(env);
        Catalog catalog = new HiveCatalog(catalogName, hiveConf.getDatabaseName(), hiveConfDir);
        tableEnv.registerCatalog(catalogName, catalog);
        tableEnv.useCatalog(catalogName);
        tableEnv.useDatabase(hiveConf.getDatabaseName());
        return tableEnv;
    }
    private SingleOutputStreamOperator<Row> generateHiveTableSource(StreamTableEnvironment tableEnv, String tableName,ComponentTableInput tableInput) throws Exception {
        String[] columns = tableInput.getColumns().split(" ");
        String[] columnsType = tableInput.getColumnsType().split(" ");
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.hive.infer-source-parallelism","false");
        Table table=tableEnv.sqlQuery("select * from "+tableName);
        DataStream<Row> dataStream = tableEnv.toDataStream(table, Row.class);
        return dataStream.map(new RichMapFunction<Row, Row>() {
            @Override
            public Row map(Row value) throws Exception {
                for(int i=0;i<columnsType.length;i++){
                    if(columnsType[i].equals(WholeVariable.MYSQL_DATETIME)
                    ||columnsType[i].equals(WholeVariable.MYSQL_TIMESTAMP)){
                        value.setField(columns[i],Timestamp.valueOf(LocalDateTime.parse(String.valueOf(value.getField(columns[i])))));
                    }
                }
                return value;
            }
        }).setParallelism(1);
    }

    public JobClient generateHiveDataSink(StreamExecutionEnvironment env, SingleOutputStreamOperator dataStreamSource, String taskName, int targetUpsertId,int taskId) throws Exception {
        SysCommonTask commonTask = commonTaskMapper.selectById(taskId);
        dataStreamSource=transformRow(dataStreamSource,targetUpsertId);
        ComponentTableUpsert tableUpsert = tableUpsertService.getById(targetUpsertId);
        DatabaseHive hiveInfo = databaseHiveService.getHiveInfo(tableUpsert.getFkDataSourceId());
        StreamTableEnvironment tableEnv=generateHiveTableEnv(env,hiveInfo);

//        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
//        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5L));

        //3.x 建表
        //判断目标数据库存不存在
        String[] tables=tableEnv.listTables();
        boolean isExisted=false;
        for(String t:tables){
            if(t.equals(tableUpsert.getTableName())){
                isExisted=true;
            }
        }
        if(!isExisted){
            //目标表不存在，建表
            String[] columns = tableUpsert.getColumns().split(" ");
            String[] columnsType = tableUpsert.getColumnsType().split(" ");
            StringBuilder sb = new StringBuilder();
            if(tableUpsert.getIsPartitioned()==1){
                //有分区
                String[] partitionCols=tableUpsert.getPartitonCol().split(" ");
                String[] partitionColsType=tableUpsert.getPartitiionColType().split(" ");
                List<String> columnsList = new LinkedList<>(Arrays.asList(columns));
                List<String> columnsTypeList = new LinkedList<>(Arrays.asList(columnsType));
                for(String s:partitionCols){
                    int index=columnsList.indexOf(s);
                    columnsList.remove(index);
                    columnsTypeList.remove(index);
                }
                int columnsLen = columns.length;
                sb.append("create table ").append(hiveInfo.getDatabaseName()).append(".").append(tableUpsert.getTableName()).append("(");
                for (int i=0;i<columnsList.size();i++) {
                    sb.append(columnsList.get(i)).append(" ").append(columnsTypeList.get(i)).append(",");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(")").append(" ").append("partitioned by( ");
                for (int i=0;i<partitionCols.length;i++) {
                    sb.append(partitionCols[i]).append(" ").append(partitionColsType[i]).append(",");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(")");
            }else{
                //无分区
                int columnsLen = columns.length;
                sb.append("create table ").append(hiveInfo.getDatabaseName()).append(".").append(tableUpsert.getTableName()).append("(");
                for (int i = 0; i < columnsLen; ++i) {
                    sb.append(columns[i]).append(" ").append(columnsType[i]).append(",");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(")");
            }
            //sb.append("STORED AS parquet TBLPROPERTIES ('sink.partition-commit.policy.kind'='metastore,success-file')");
            System.out.println("目标表建表语句"+sb.toString());
            //目标表建表语句create table test.datetest(id int,name string) partitioned by( date1 string)
            tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
            //目标数据库不存在，建数据库
            tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS "+hiveInfo.getDatabaseName());
            //tableEnv.executeSql("DROP TABLE IF EXISTS "+hiveInfo.getDatabaseName()+"."+tableUpsert.getTableName());
            tableEnv.executeSql(sb.toString());
        }
        //设置hive sink的并行度
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        int parallelism=commonTask.getParallelism() <= maxParallelism ? commonTask.getParallelism(): maxParallelism;
        //这块可以修改参数取值，寻找更好的性能
        if(commonTask.getSourceDataType().equals(WholeVariable.KAFKA)){
            tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
            tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(5L));
            tableEnv.executeSql("alter table "+hiveInfo.getDatabaseName()+"."+tableUpsert.getTableName()+" set tblproperties('sink.partition-commit.policy.kind'='metastore,success-file','sink.parallelism'='"+parallelism+"'" +
                ",'sink.partition-commit.trigger'='partition-time'" +
                ",'sink.partition-commit.delay'='10s'" +
                ",'sink.partition-commit.delay'='10s'" +
                ",'sink.semantic' = 'exactly-once'" +
                ",'sink.rolling-policy.file-size'='128MB'" +
                ",'sink.rolling-policy.rollover-interval' ='10s'" +
                ",'sink.rolling-policy.check-interval'='10s'" +
                    ")");
        }else{
            tableEnv.executeSql("alter table "+hiveInfo.getDatabaseName()+"."+tableUpsert.getTableName()+" set tblproperties('sink.partition-commit.policy.kind'='metastore,success-file','sink.parallelism'='"+parallelism+"'" +
                    ")");
        }
        //数据导入
        //这块需要添加检查数据类型！！！！！！！！！！！！！！！！！！！
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        tableEnv.createTemporaryView("target",dataStreamSource);
        tableUpsert.getColumns().replaceAll(" ",",");
        //分区表也可以共用下面插入语句
        String sql="insert into "+tableUpsert.getTableName()+"("+tableUpsert.getColumns().replaceAll(" ",",")+")"+" select "+tableUpsert.getColumns().replaceAll(" ",",")+" from target";

        System.out.println(sql);
        JobClient jobClient = tableEnv.executeSql(sql).getJobClient().get();
        return jobClient;

    }

    public SingleOutputStreamOperator<Row> transformRow(SingleOutputStreamOperator<Row> dataStreamSource, int targetUpsertId) throws Exception{
        ComponentTableUpsert tableUpsert = tableUpsertService.getById(targetUpsertId);
        String inputColumns = tableUpsert.getColumns();
        String inputColumnsType = tableUpsert.getColumnsType();
        String[] inputColumnsArr = inputColumns.split(" ");
        String[] inputColumnsTypeArr = inputColumnsType.split(" ");
        List<String> inputColumnsList = new LinkedList<>(Arrays.asList(inputColumnsArr));
        List<String> inputColumnsTypeList = new LinkedList<>(Arrays.asList(inputColumnsTypeArr));
        RowTypeInfo rowTypeInfo = getRowTypeInfo(inputColumnsList, inputColumnsTypeList, null, null);
        String[] dateFields = getDateFields(targetUpsertId);

        return dataStreamSource
//                .map(new RichMapFunction<Row, Row>() {
//                    @Override
//                    public Row map(Row value) throws Exception {
//                        for (int i=0; i<dateFields.length; ++i) {
//                            value.setField(dateFields[i], String.valueOf(value.getField(dateFields[i])));
//                        }
//                        return value;
//                    }
//                })
                .returns(rowTypeInfo);
    }
    private RowTypeInfo getRowTypeInfo(List<String> inputColumnList, List<String> inputColumnTypeList, String[] saveColumnNameArr, String[] saveColumnTypeArr) throws Exception {
        List<String> nameList = new ArrayList<>(inputColumnList);
        if(saveColumnNameArr!=null){
            nameList.addAll(new ArrayList<>(Arrays.asList(saveColumnNameArr)));
        }
        String[] nameArr = nameList.toArray(new String[0]);
        List<String> typeList = new ArrayList<>(inputColumnTypeList);
        if(saveColumnTypeArr!=null){
            typeList.addAll(new ArrayList<>(Arrays.asList(saveColumnTypeArr)));
        }
        String[] typeArr = typeList.toArray(new String[0]);

        int len = nameArr.length;
        String[] fieldName = new String[len];
        TypeInformation[] typeInformation = new TypeInformation[len];

        for (int i=0; i<len; ++i) {
            fieldName[i] = nameArr[i];
            switch (typeArr[i]) {
                case WholeVariable.HIVE_STRING:
                    typeInformation[i] = BasicTypeInfo.STRING_TYPE_INFO;
                    break;
                case WholeVariable.HIVE_INT:
                    typeInformation[i] = BasicTypeInfo.INT_TYPE_INFO;
                    break;
                case WholeVariable.FLOAT:
                    typeInformation[i] = BasicTypeInfo.FLOAT_TYPE_INFO;
                    break;
                case WholeVariable.MYSQL_DATE:
                case WholeVariable.MYSQL_DATETIME:
                case WholeVariable.MYSQL_TIMESTAMP:
//                    typeInformation[i] = BasicTypeInfo.STRING_TYPE_INFO;
                    typeInformation[i] = SqlTimeTypeInfo.TIMESTAMP;
                    break;
                default:
                    throw new Exception("unknown type of split/input");
            }
        }
        return new RowTypeInfo(typeInformation, fieldName);
    }

    private String[] getDateFields(int targetUpsertId) {
        ComponentTableUpsert tableUpsert = tableUpsertService.getById(targetUpsertId);
        List<String> dateList = new ArrayList<>();
        String[] columns = tableUpsert.getColumns().split(" ");
        String[] columnsType = tableUpsert.getColumnsType().split(" ");
        int len = columns.length;
        for (int i=0; i<len; ++i) {
            if (columnsType[i].equals(WholeVariable.MYSQL_DATE) ||
                columnsType[i].equals(WholeVariable.MYSQL_DATETIME) ||
                columnsType[i].equals(WholeVariable.MYSQL_TIMESTAMP) ||
                columnsType[i].equals(WholeVariable.MYSQL_TIME)) {
                dateList.add(columns[i]);
            }
        }
        return dateList.toArray(new String[0]);
    }
}
