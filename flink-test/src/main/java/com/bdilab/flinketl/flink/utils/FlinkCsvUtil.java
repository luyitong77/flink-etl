package com.bdilab.flinketl.flink.utils;

import com.bdilab.flinketl.entity.ComponentCsvInput;
import com.bdilab.flinketl.entity.ComponentCsvOutput;
import com.bdilab.flinketl.entity.DatabaseCsv;
import com.bdilab.flinketl.entity.DatabaseHdfs;
import com.bdilab.flinketl.service.ComponentCsvInputService;
import com.bdilab.flinketl.service.ComponentCsvOutputService;
import com.bdilab.flinketl.service.DatabaseCsvService;
import com.bdilab.flinketl.service.DatabaseHdfsService;
import com.bdilab.flinketl.utils.HdfsClient;
import com.bdilab.flinketl.utils.WholeVariable;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

import static org.apache.flink.table.api.Expressions.$;

/**
 *
 * 用于配置Flink数据流中与csv文件相关的工具类
 *
 * @author hcyong
 * @date 2021/10/8
 */
@Slf4j
@Component
public class FlinkCsvUtil {

    private static final String OUTPUT_TABLE_NAME = "outputTable";

    @Value("${output.csv-path}")
    String outputPath;

    @Resource
    DatabaseCsvService csvService;
    @Resource
    DatabaseHdfsService hdfsService;
    @Resource
    ComponentCsvInputService csvInputService;
    @Resource
    ComponentCsvOutputService csvOutputService;
    @Resource
    HdfsClient hdfsClient;

    public DataStreamSource<Row> generateCsvDataSource(StreamExecutionEnvironment env, int sourceInputId) throws Exception {
        CsvTableSource csvTableSource = generateCsvTableSource(sourceInputId);

        DataStream<Row> dataStream = csvTableSource.getDataStream(env);

        return (DataStreamSource<Row>) dataStream;
    }

    private CsvTableSource generateCsvTableSource(int sourceInputId) throws Exception {
        String inputFilePath = generateCsvFilePath(sourceInputId);
        ComponentCsvInput csvInput = csvInputService.getById(sourceInputId);
        DatabaseCsv databaseCsv = csvService.getById(csvInput.getFkDataSourceId());
        DatabaseHdfs databaseHdfs = hdfsService.getById(databaseCsv.getFkHdfsDataSourceId());

        String delimiter = csvInput.getDelimiter();
        String[] columnsType = csvInput.getColumnsType().split(" ");
        String[] columns = hdfsClient.getFirstLine(databaseHdfs.getId(), inputFilePath).split(delimiter);

        int len = columns.length;
        TypeInformation[] typeInformation = new TypeInformation[len];
        for (int i = 0; i < len; ++i) {
            switch (columnsType[i]) {
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
                    throw new Exception("unknown type of csv input");
            }
        }

//        RowTypeInfo[] rowTypeInfo = {new RowTypeInfo(typeInformation, columns)};
//        RowCsvInputFormat csvInputFormat = new RowCsvInputFormat(new Path(inputFilePath), rowTypeInfo, "\n", delimiter);
//        csvInputFormat.setSkipFirstLineAsHeader(true);

        CsvTableSource csvTableSource = new CsvTableSource(inputFilePath, columns, typeInformation,
                delimiter, "\n", null, true, null, false);
        return csvTableSource;
    }

    private String generateCsvFilePath(int sourceInputId) {
        ComponentCsvInput csvInput = csvInputService.getById(sourceInputId);
        int csvDataSourceId = csvInput.getFkDataSourceId();
        DatabaseCsv databaseCsv = csvService.getById(csvDataSourceId);

        int hdfsDataSourceId = databaseCsv.getFkHdfsDataSourceId();
        String inputHdfsPath = csvInputService.getInputHdfsPath(hdfsDataSourceId);

        String inputFilePath = csvInputService.getInputFilePath(inputHdfsPath, databaseCsv.getFileName());
        return inputFilePath;
    }

    public JobClient generateCsvDataSink(StreamExecutionEnvironment env, SingleOutputStreamOperator dataStreamSource, String taskName, int targetUpsertId) throws Exception {
        String sql = generateCsvOutputSql(targetUpsertId);
        dataStreamSource = dataStreamSource.filter(new RichFilterFunction() {
            @Override
            public boolean filter(Object value) throws Exception {
                return true;
            }
        }).setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        tableEnvironment.getConfig().getConfiguration().setString("pipeline.name", taskName);
        tableEnvironment.executeSql(sql);

        List<Expression> expressions = generateCsvOutputExprColumns(targetUpsertId);
        Table outputTable = tableEnvironment.fromDataStream(dataStreamSource, expressions.toArray(new Expression[0]));
        return outputTable.executeInsert(OUTPUT_TABLE_NAME).getJobClient().get();
    }

    private String generateCsvOutputSql(int targetUpsertId) throws Exception {
        ComponentCsvOutput csvOutput = csvOutputService.getById(targetUpsertId);
        DatabaseHdfs databaseHdfs = hdfsService.getById(csvOutput.getFkHdfsDataSourceId());
        hdfsClient.createDirectory(databaseHdfs.getId(), outputPath);

        String[] columns = csvOutput.getColumns().split(" ");
        String[] columnsType = csvOutput.getColumnsType().split(" ");
        int columnsLen = columns.length;

        if (columns.length != columnsType.length) {
            throw new Exception("input columns and type don't match");
        }

        StringBuilder sb = new StringBuilder();
        sb.append("create table " + OUTPUT_TABLE_NAME + " (");
        for (int i = 0; i < columnsLen; ++i) {
            sb.append(columns[i]).append(" ").append(columnsType[i].toLowerCase(Locale.ROOT)).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);

        String connectorSQL = ") with (" +
                "'connector.type' = 'filesystem'," +
                "'format.type' = 'csv'," +
                "'format.field-delimiter' = '" + csvOutput.getDelimiter() + "'," +
                "'connector.path' = 'hdfs://%s:%d" + outputPath + "/%s'" +
                ")";
        String connector = String.format(connectorSQL,
                databaseHdfs.getHdfsIp(),
                databaseHdfs.getHdfsPort(),
                csvOutput.getFileName());
        sb.append(connector);
        return sb.toString();

    }

    private List<Expression> generateCsvOutputExprColumns(int targetUpsertId) {
        List<Expression> expressions = new LinkedList<>();
        ComponentCsvOutput csvOutput = csvOutputService.getById(targetUpsertId);
        String[] columns = csvOutput.getColumns().split(" ");
        for (String column : columns) {
            expressions.add($(column));
        }
        return expressions;
    }
}
