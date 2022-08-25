package com.bdilab.flinketl.flink.utils;

import com.bdilab.flinketl.entity.ComponentTableInput;
import com.bdilab.flinketl.entity.ComponentTableUpsert;
import com.bdilab.flinketl.entity.DatabaseMysql;
import com.bdilab.flinketl.entity.DatabaseSqlserver;
import com.bdilab.flinketl.entity.DatabaseOracle;
import com.bdilab.flinketl.service.ComponentTableInputService;
import com.bdilab.flinketl.service.ComponentTableUpsertService;
import com.bdilab.flinketl.service.DatabaseMysqlService;
import com.bdilab.flinketl.service.DatabaseSqlServerService;
import com.bdilab.flinketl.service.DatabaseOracleService;
import com.bdilab.flinketl.utils.WholeVariable;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 *
 * 用于配置Flink数据流中与JDBC相关的工具类
 *
 * @author hcyong
 * @date 2021/10/30
 */
@Component
public class FlinkJdbcUtil {

    @Resource
    ComponentTableInputService tableInputService;
    @Resource
    ComponentTableUpsertService tableUpsertService;
    @Resource
    DatabaseMysqlService mysqlService;
    @Resource
    DatabaseSqlServerService sqlServerService;

    public JdbcInputFormat generateJdbcInputFormat(int sourceInputId, String sourceType) throws Exception{
        ComponentTableInput tableInput = tableInputService.getById(sourceInputId);
        Properties jdbcProperties = buildJdbcInputProperties(sourceType, tableInput);
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

    public JdbcOutputFormat buildJdbcOutputFormat(int targetUpsertId, String targetType) throws Exception {
        ComponentTableUpsert tableUpsert = tableUpsertService.getById(targetUpsertId);
        Properties jdbcProperties = buildJdbcOutputProperties(targetType, tableUpsert);
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

    private Properties buildJdbcInputProperties(String sourceType, ComponentTableInput tableInput) throws Exception {
        Properties jdbcProperties = new Properties();
        String driverName = getDriverClassName(sourceType);
        String sql = generateSelectSql(tableInput);
        Map<String, String> jdbcInfo = buildJdbcConnInputInfo(sourceType, tableInput);

        jdbcProperties.put("driver", driverName);
        jdbcProperties.put("sql", sql);
        jdbcProperties.put("url", jdbcInfo.get("url"));
        jdbcProperties.put("username", jdbcInfo.get("username"));
        jdbcProperties.put("password", jdbcInfo.get("password"));

        return jdbcProperties;
    }

    private Properties buildJdbcOutputProperties(String targetType, ComponentTableUpsert tableUpsert) throws Exception {
        Properties jdbcProperties = new Properties();
        String driverName = getDriverClassName(targetType);
        String sql = generateInsertSql(tableUpsert);
        Map<String, String> jdbcInfo = buildJdbcConnOutputInfo(targetType, tableUpsert);

        jdbcProperties.put("driver", driverName);
        jdbcProperties.put("sql", sql);
        jdbcProperties.put("url", jdbcInfo.get("url"));
        jdbcProperties.put("username", jdbcInfo.get("username"));
        jdbcProperties.put("password", jdbcInfo.get("password"));

        return jdbcProperties;
    }

    public String getDriverClassName(String targetType) throws Exception{
        switch (targetType) {
            case WholeVariable.MYSQL:
                return WholeVariable.MYSQL_DRIVER_NAME;
//            case WholeVariable.ORACLE:
//                return WholeVariable.ORACLE_DRIVER_NAME;
            case WholeVariable.SQLSERVER:
                return WholeVariable.SQLSERVER_DRIVER_NAME;
            default:
                throw new Exception("Unsupported jdbc driver of targetType: " + targetType);
        }
    }

    /**
     * 生成select
     * @param tableInput
     * @return sql
     */
    public String generateSelectSql(ComponentTableInput tableInput) {
        String customSql = tableInput.getCustomSql();
        if (customSql != null) {
            return customSql;
        } else {
            String[] columns = tableInput.getColumns().split(" ");
            String tableName = tableInput.getTableName();
            String rawSql = "select %s from %s %s";
            return generateSql(rawSql, columns, tableName, false);
        }
    }

    /**
     * 生成insert
     * @param tableUpsert
     * @return sql
     */
    public String generateInsertSql(ComponentTableUpsert tableUpsert) {
        String[] columns = tableUpsert.getColumns().split(" ");
        String tableName = tableUpsert.getTableName();
        String rawSql = "insert into %s %s values %s";
        return generateSql(rawSql, columns, tableName, true);
    }

    private String generateSql(String rawSql, String[] columns, String tableName, boolean isInsertSql) {
        StringBuilder columnSql = new StringBuilder();
        StringBuilder valueSql = new StringBuilder();
        columnSql.append("(");
        valueSql.append("(");
        for (String s : columns) {
            columnSql.append(s).append(",");
            valueSql.append("?").append(",");
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

    public Map<String, String> buildJdbcConnInputInfo(String sourceType, ComponentTableInput tableInput) throws Exception {
        int fkDataSourceId = tableInput.getFkDataSourceId();
        switch (sourceType) {
            case WholeVariable.MYSQL:
                return getMysqlInfo(fkDataSourceId);
            case WholeVariable.SQLSERVER:
                return getSqlServerInfo(fkDataSourceId);
            default:
                throw new Exception("Unsupported jdbc connection info of sourceType: " + sourceType);
        }
    }

    public Map<String, String> buildJdbcConnOutputInfo(String targetType, ComponentTableUpsert tableUpsert) throws Exception {
        int fkDataSourceId = tableUpsert.getFkDataSourceId();
        switch (targetType) {
            case WholeVariable.MYSQL:
                return getMysqlInfo(fkDataSourceId);
            case WholeVariable.SQLSERVER:
                return getSqlServerInfo(fkDataSourceId);
            default:
                throw new Exception("Unsupported jdbc connection info of targetType: " + targetType);
        }
    }

    public Map<String, String> getMysqlInfo(int mysqlId) {
        String rawUrl = "jdbc:mysql://%s:%d/%s?useUnicode=true&useSSL=false&characterEncoding=utf8&rewriteBatchedStatements=true&useServerPrepStmts=false";
        DatabaseMysql databaseMysql = mysqlService.getById(mysqlId);
        String databaseName = databaseMysql.getDatabaseName();
        String hostname = databaseMysql.getHostname();
        int port = databaseMysql.getPort();
        String url = String.format(rawUrl, hostname, port, databaseName);

        String username = databaseMysql.getUsername();
        String password = databaseMysql.getPassword();

        Map<String, String> map = new HashMap<>();
        map.put("url", url);
        map.put("username", username);
        map.put("password", password);
        return map;
    }

    public Map<String, String> getSqlServerInfo(int sqlServerId) {
        String rawUrl = "jdbc:sqlserver://%s:%d;DatabaseName=%s";
        DatabaseSqlserver databaseSqlServer = sqlServerService.getById(sqlServerId);
        String databaseName = databaseSqlServer.getDatabaseName();
        String hostname = databaseSqlServer.getHostname();
        int port = databaseSqlServer.getPort();
        String url = String.format(rawUrl, hostname, port, databaseName);

        String username = databaseSqlServer.getUsername();
        String password = databaseSqlServer.getPassword();

        Map<String, String> map = new HashMap<>();
        map.put("url", url);
        map.put("username", username);
        map.put("password", password);
        return map;
    }

    public RowTypeInfo buildRowTypeInfo(ComponentTableInput tableInput) throws Exception {
        String[] fieldNames = tableInput.getColumns().split(" ");
        String[] columnsType = tableInput.getColumnsType().split(" ");
        int len = columnsType.length;
        TypeInformation[] typeInfo = new TypeInformation[len];
        for (int i = 0; i < len; ++i) {
            switch (columnsType[i]) {
                case WholeVariable.VARCHAR:
                case WholeVariable.ORACLE_NVARCHAR2:
                    typeInfo[i] = BasicTypeInfo.STRING_TYPE_INFO;
                    break;
                case WholeVariable.INT:
                    typeInfo[i] = BasicTypeInfo.INT_TYPE_INFO;
                    break;
                case WholeVariable.ORACLE_NUMBER:
                    typeInfo[i] = BasicTypeInfo.BIG_DEC_TYPE_INFO;
                    break;
                case WholeVariable.FLOAT:
                    typeInfo[i] = BasicTypeInfo.FLOAT_TYPE_INFO;
                    break;
                case WholeVariable.MYSQL_DATE:
                    typeInfo[i] = SqlTimeTypeInfo.DATE;
                    break;
                case WholeVariable.MYSQL_DATETIME:
                case WholeVariable.MYSQL_TIMESTAMP:
                    typeInfo[i] = SqlTimeTypeInfo.TIMESTAMP;
                    break;
                default:
                    throw new Exception("Unsupported output type: " + columnsType[i]);
            }
        }
        return new RowTypeInfo(typeInfo, fieldNames);
    }

    public int[] buildSqlType(ComponentTableUpsert tableUpsert) throws Exception {
        String[] columnsType = tableUpsert.getColumnsType().split(" ");
        int len = columnsType.length;
        int[] sqlTypes = new int[len];
        for (int i=0; i<len; ++i) {
            switch (columnsType[i]) {
                case WholeVariable.ORACLE_NVARCHAR2:
                case WholeVariable.VARCHAR:
                    sqlTypes[i] = Types.VARCHAR;
                    break;
                case WholeVariable.INT:
                    sqlTypes[i] = Types.INTEGER;
                    break;
                case WholeVariable.FLOAT:
                    sqlTypes[i] = Types.FLOAT;
                    break;
                case WholeVariable.ORACLE_NUMBER:
                    sqlTypes[i] = Types.DECIMAL;
                    break;
                // yyyy-MM-dd HH:mm:ss SSSS
                case WholeVariable.DATE:
                case WholeVariable.MYSQL_DATETIME:
                case WholeVariable.MYSQL_TIME:
                    sqlTypes[i] = Types.TIMESTAMP_WITH_TIMEZONE;
                    break;
                default:
                    throw new Exception("Unsupported output type: " + columnsType[i]);
            }
        }
        return sqlTypes;
    }

    //oracle
//
//    /**
//     * 生成select
//     * @param tableInput
//     * @return sql
//     */
//    public String generateOracleSelectSql(ComponentTableInput tableInput) {
//        String customSql = tableInput.getCustomSql();
//        if (customSql != null) {
//            return customSql;
//        } else {
//            String[] columns = tableInput.getColumns().split(" ");
//            String tableName = tableInput.getTableName();
//            String rawSql = "select %s from %s %s";
//            return generateOracleSql(rawSql, columns, tableName, false);
//        }
//    }
//
//    /**
//     * 生成insert
//     * @param tableUpsert
//     * @return sql
//     */
//    public String generateOracleInsertSql(ComponentTableUpsert tableUpsert) {
//        String[] columns = tableUpsert.getColumns().split(" ");
//        String tableName = tableUpsert.getTableName();
//        String rawSql = "insert into %s %s values %s";
//        return generateOracleSql(rawSql, columns, tableName, true);
//    }
//
//    private String generateOracleSql(String rawSql, String[] columns, String tableName, boolean isInsertSql) {
//        StringBuilder columnSql = new StringBuilder();
//        StringBuilder valueSql = new StringBuilder();
//        columnSql.append("(");
//        valueSql.append("(");
//        for (String column : columns) {
//            columnSql.append("\"").append(column).append("\"").append(",");
//            valueSql.append("?,");
//        }
//        columnSql.deleteCharAt(columnSql.length()-1).append(")");
//        valueSql.deleteCharAt(valueSql.length()-1).append(")");
//
//        if (isInsertSql) {
//            return String.format(rawSql, tableName, columnSql, valueSql);
//        } else {
//            return String.format(rawSql, tableName, columnSql.substring(1, columnSql.length()-1), "");
//        }
//    }

    //    public Map<String, String> getOracleInfo(int oracleId) {
//        DatabaseOracle databaseOracle = oracleService.getById(oracleId);
//        String rawUrl = databaseOracle.getIsServiceName() == 1
//                ? "jdbc:oracle:thin:%s:%d/%s?useUnicode=true&useSSL=false&characterEncoding=utf8&rewriteBatchedStatements=true&useServerPrepStmts=false"
//                : "jdbc:oracle:thin:%s:%d:%s?useUnicode=true&useSSL=false&characterEncoding=utf8&rewriteBatchedStatements=true&useServerPrepStmts=false";
//
//        String databaseName = databaseOracle.getDatabaseName();
//        String hostname = databaseOracle.getHostname();
//        int port = databaseOracle.getPort();
//        String url = String.format(rawUrl, hostname, port, databaseName);
//
//        String username = databaseOracle.getUsername();
//        String password = databaseOracle.getPassword();
//
//        Map<String, String> map = new HashMap<>();
//        map.put("url", url);
//        map.put("username", username);
//        map.put("password", password);
//        return map;
//    }
}
