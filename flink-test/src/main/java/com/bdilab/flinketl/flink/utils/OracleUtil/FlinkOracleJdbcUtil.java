package com.bdilab.flinketl.flink.utils.OracleUtil;

import com.bdilab.flinketl.entity.ComponentTableInput;
import com.bdilab.flinketl.entity.ComponentTableUpsert;
import com.bdilab.flinketl.entity.DatabaseOracle;
import com.bdilab.flinketl.flink.utils.FlinkJdbcUtil;
import com.bdilab.flinketl.service.ComponentTableInputService;
import com.bdilab.flinketl.service.ComponentTableUpsertService;
import com.bdilab.flinketl.service.DatabaseOracleService;
import com.bdilab.flinketl.utils.WholeVariable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author hcyong
 * @date 2021/10/30
 */
@Component
public class FlinkOracleJdbcUtil {

    @Resource
    ComponentTableInputService tableInputService;
    @Resource
    ComponentTableUpsertService tableUpsertService;
    @Resource
    DatabaseOracleService oracleService;
    @Resource
    FlinkJdbcUtil flinkJdbcUtil;

    public JdbcInputFormat generateJdbcInputFormat(int sourceInputId) throws Exception{
        ComponentTableInput tableInput = tableInputService.getById(sourceInputId);
        Properties jdbcProperties = buildJdbcInputProperties(tableInput);
        RowTypeInfo rowTypeInfo = buildRowTypeInfo(tableInput);
        return JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(jdbcProperties.getProperty("driver"))
                .setDBUrl(jdbcProperties.getProperty("url"))
                .setUsername(jdbcProperties.getProperty("username"))
                .setPassword(jdbcProperties.getProperty("password"))
                .setQuery(jdbcProperties.getProperty("sql"))
                .setRowTypeInfo(rowTypeInfo)
                .finish();
    }

    public JdbcOutputFormat buildJdbcOutputFormat(int targetUpsertId) throws Exception {
        ComponentTableUpsert tableUpsert = tableUpsertService.getById(targetUpsertId);
        Properties jdbcProperties = buildJdbcOutputProperties(tableUpsert);
        int[] sqlTypes = buildSqlType(tableUpsert);
        return JdbcOutputFormat.buildJdbcOutputFormat()
                .setDrivername(jdbcProperties.getProperty("driver"))
                .setDBUrl(jdbcProperties.getProperty("url"))
                .setUsername(jdbcProperties.getProperty("username"))
                .setPassword(jdbcProperties.getProperty("password"))
                .setQuery(jdbcProperties.getProperty("sql"))
                .setSqlTypes(sqlTypes)
                .finish();
    }

    private Properties buildJdbcInputProperties(ComponentTableInput tableInput) {
        Properties jdbcProperties = new Properties();
        String sql = generateOracleSelectSql(tableInput);
        Map<String, String> jdbcInfo = getOracleInfo(tableInput.getFkDataSourceId());

        jdbcProperties.put("driver", WholeVariable.ORACLE_DRIVER_NAME);
        jdbcProperties.put("sql", sql);
        jdbcProperties.put("url", jdbcInfo.get("url"));
        jdbcProperties.put("username", jdbcInfo.get("username"));
        jdbcProperties.put("password", jdbcInfo.get("password"));

        return jdbcProperties;
    }

    private Properties buildJdbcOutputProperties(ComponentTableUpsert tableUpsert) {
        Properties jdbcProperties = new Properties();
        String sql = generateOracleInsertSql(tableUpsert);
        Map<String, String> jdbcInfo = getOracleInfo(tableUpsert.getFkDataSourceId());

        jdbcProperties.put("driver", WholeVariable.ORACLE_DRIVER_NAME);
        jdbcProperties.put("sql", sql);
        jdbcProperties.put("url", jdbcInfo.get("url"));
        jdbcProperties.put("username", jdbcInfo.get("username"));
        jdbcProperties.put("password", jdbcInfo.get("password"));

        return jdbcProperties;
    }

    /**
     * 生成select
     * @param tableInput
     * @return sql
     */
    public String generateOracleSelectSql(ComponentTableInput tableInput) {
        String customSql = tableInput.getCustomSql();
        if (customSql != null) {
            return customSql;
        } else {
            String[] columns = tableInput.getColumns().split(" ");
            String tableName = tableInput.getTableName();
            String rawSql = "select %s from %s %s";
            return generateOracleSql(rawSql, columns, tableName, false);
        }
    }

    /**
     * 生成insert
     * @param tableUpsert
     * @return sql
     */
    public String generateOracleInsertSql(ComponentTableUpsert tableUpsert) {
        String[] columns = tableUpsert.getColumns().split(" ");
        String tableName = tableUpsert.getTableName();
        String rawSql = "insert into %s %s values %s";
        return generateOracleSql(rawSql, columns, tableName, true);
    }

    private String generateOracleSql(String rawSql, String[] columns, String tableName, boolean isInsertSql) {
        StringBuilder columnSql = new StringBuilder();
        StringBuilder valueSql = new StringBuilder();
        columnSql.append("(");
        valueSql.append("(");
        for (String column : columns) {
            columnSql.append("\"").append(column).append("\"").append(",");
            valueSql.append("?,");
        }
        columnSql.deleteCharAt(columnSql.length()-1).append(")");
        valueSql.deleteCharAt(valueSql.length()-1).append(")");

        if (isInsertSql) {
            //insert
            return String.format(rawSql, tableName, columnSql, valueSql);
        } else {
            //select
            return String.format(rawSql, columnSql.substring(1, columnSql.length()-1), tableName, "");
        }
    }

    public Map<String, String> getOracleInfo(int oracleId) {
        DatabaseOracle databaseOracle = oracleService.getById(oracleId);
        String rawUrl = databaseOracle.getIsServiceName() == 1
                ? "jdbc:oracle:thin:%s:%d/%s?useUnicode=true&useSSL=false&characterEncoding=utf8&rewriteBatchedStatements=true&useServerPrepStmts=false"
                : "jdbc:oracle:thin:%s:%d:%s?useUnicode=true&useSSL=false&characterEncoding=utf8&rewriteBatchedStatements=true&useServerPrepStmts=false";

        String databaseName = databaseOracle.getDatabaseName();
        String hostname = databaseOracle.getHostname();
        int port = databaseOracle.getPort();
        String url = String.format(rawUrl, hostname, port, databaseName);

        String username = databaseOracle.getUsername();
        String password = databaseOracle.getPassword();

        Map<String, String> map = new HashMap<>();
        map.put("url", url);
        map.put("username", username);
        map.put("password", password);
        return map;
    }

    public RowTypeInfo buildRowTypeInfo(ComponentTableInput tableInput) throws Exception {
        return flinkJdbcUtil.buildRowTypeInfo(tableInput);
    }

    public int[] buildSqlType(ComponentTableUpsert tableUpsert) throws Exception {
        return flinkJdbcUtil.buildSqlType(tableUpsert);
    }
}
