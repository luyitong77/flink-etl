package com.bdilab.flinketl.utils;

import com.bdilab.flinketl.entity.DatabaseMysql;
import com.bdilab.flinketl.mapper.DatabaseMysqlMapper;
import com.bdilab.flinketl.utils.common.entity.ResultCode;
import com.bdilab.flinketl.utils.common.exception.CommonException;
import com.bdilab.flinketl.utils.exception.DatabaseNotExistException;
import com.bdilab.flinketl.utils.exception.InfoNotInDatabaseException;
import com.bdilab.flinketl.utils.exception.TableNotExistException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Author: cyz
 * Date: 2019/10/12
 * Description:
 */
@Slf4j
@Component
public class MySqlClient {

    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DEFAULT_DATABASE = "information_schema";
    public static final String COLUMN_NAME = "column_names";
    public static final String COLUMN_TYPE = "column_types";

    @Resource
    private DatabaseMysqlMapper databaseMysqlMapper;
//    @Resource
//    private GenerateTaskIdAndDataBaseIdAndComponentId generateTaskIdAndDataBaseIdAndComponentId;


    public Connection getConnection(DatabaseMysql databaseMysql) throws SQLException {
        Connection connection = null;
        try {
            Class.forName(MYSQL_DRIVER);
            String databaseName = (databaseMysql.getDatabaseName() == null) ? DEFAULT_DATABASE : databaseMysql.getDatabaseName();
            String url = "jdbc:mysql://" + databaseMysql.getHostname() + ":" + databaseMysql.getPort() + "/" + databaseName
                    + "?characterEncoding=utf8&useSSL=false&serverTimezone=GMT%2B8&useOldAliasMetadataBehavior=true";
            connection = DriverManager.getConnection(url, databaseMysql.getUsername(), databaseMysql.getPassword());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public String testConnection(DatabaseMysql databaseMysql) throws SQLException {
        Connection connection = getConnection(databaseMysql);
        if (connection != null) {
            connection.close();
            return "success";
        }
        return "fail";
    }
//
//    /**
//     * ??????mysql??????????????????
//     *
//     * @param databaseId
//     * @return
//     */
//    public DatabaseMysql getConnectionConfig(long databaseId) {
//        if (!databaseMysqlMapper.selectById(databaseId)) {
//            return null;
//        }
//        return databaseMysqlMapper.getOne(databaseId);
//    }
//
//    public long saveDatabaseConfig(MysqlDatabaseConfig mysqlDatabaseConfig) {
//        long databaseId = generateTaskIdAndDataBaseIdAndComponentId.generateDatabaseId(WholeVariable.MYSQL);
//        if (databaseId < 1) {
//            return -1;
//        }
//        mysqlDatabaseConfig.setId(databaseId);
//        databaseMysqlMapper.save(mysqlDatabaseConfig);
//        return databaseId;
//    }
//
//    public boolean editDatabaseConfig(MysqlDatabaseConfig mysqlDatabaseConfig) {
//        if (!databaseMysqlMapper.existsById(mysqlDatabaseConfig.getId())) {
//            throw new InfoNotInDatabaseException("??????????????????????????????");
//        }
//        databaseMysqlMapper.save(mysqlDatabaseConfig);
//        return true;
//    }
//
//    /**
//     * ??????mysql??????????????????
//     *
//     * @param databaseId
//     * @return
//     */
//    public boolean deleteConnection(long databaseId) {
//        if (!databaseMysqlMapper.existsById(databaseId)) {
//            return true;
//        }
//        databaseMysqlMapper.deleteById(databaseId);
//        return true;
//    }
//
//    public List<FieldAndType> getFieldAndTypeFromSql(long databaseId, String sql) throws SQLException {
//        MysqlDatabaseConfig databaseConfig = getConnectionConfig(databaseId);
//        if (databaseConfig == null) {
//            throw new InfoNotInDatabaseException("mysql???????????????");
//        }
//        System.out.println("sql: " + sql);
//        Connection connection = getConnection(databaseConfig);
//        List<FieldAndType> results = ParseSql.getSqlFields(connection, sql,WholeVariable.MYSQL);
//        connection.close();
//        return results;
//    }
//
//    public Map<String,String> getSingleRowFromDatabase(long databaseId,String sql) throws SQLException{
//        MysqlDatabaseConfig databaseConfig = getConnectionConfig(databaseId);
//        if (databaseConfig == null) {
//            throw new InfoNotInDatabaseException("mysql???????????????");
//        }
//        System.out.println("sql: " + sql);
//        Connection connection = getConnection(databaseConfig);
//        SqlExcuter sqlExcuter = new SqlExcuter();
//        sqlExcuter.setConnection(connection);
//        Map<String,String> results = sqlExcuter.executeQuerySingleRow(sql);
//        sqlExcuter.closeAll();
//        return results;
//    }

    public List<String> getAllDatabases(DatabaseMysql databaseMysql) throws SQLException {
        Connection connection = getConnection(databaseMysql);
        List<String> results = getAllDatabases(connection);

        connection.close();
        return results;
    }

    private List<String> getAllDatabases(Connection connection) throws SQLException {
        ResultSet rs = null;
        PreparedStatement prst;
        List<String> databaseNames = new ArrayList<>();

        prst = connection.prepareStatement("SHOW DATABASES;");
        rs = prst.executeQuery();
        while (rs.next()) {
            databaseNames.add(rs.getString(1));
        }
        prst.close();
        return databaseNames;
    }

    public List<String> getAllTables(int id) throws SQLException {
        DatabaseMysql databaseMysql = databaseMysqlMapper.selectById(id);
        if (databaseMysql == null) {
            throw new InfoNotInDatabaseException("????????????????????????: ");
        }
        Connection connection = getConnection(databaseMysql);
        List<String> databases = getAllDatabases(connection);
        if (!databases.contains(databaseMysql.getDatabaseName())) {
            System.out.println("???????????????");
            throw new DatabaseNotExistException("???????????????");
        }

        List<String> results = getAllTables(connection, databaseMysql.getDatabaseName());
        connection.close();
        return results;
    }

    private List<String> getAllTables(Connection connection, String databaseName) throws SQLException {
        ResultSet rs = null;
        PreparedStatement prst;
        //????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
        String sql = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE table_type = 'BASE TABLE'and TABLE_SCHEMA = '" + databaseName + "';";
        List<String> tableNames = new ArrayList<>();

        prst = connection.prepareStatement(sql);
        rs = prst.executeQuery();
        while (rs.next()) {
            tableNames.add(rs.getString(1));
        }
        prst.close();
        return tableNames;
    }
//
//    /**
//     * ??????????????????????????????????????????
//     *
//     * @param id
//     * @param tableName
//     * @return
//     * @throws SQLException
//     */
//    public Boolean containsTable(long id, String tableName) throws SQLException {
//        if (!databaseMysqlMapper.existsById(id)) {
//            throw new InfoNotInDatabaseException("????????????????????????: ");
//        }
//        MysqlDatabaseConfig databaseConfig = databaseMysqlMapper.getOne(id);
//        Connection connection = getConnection(databaseConfig);
//        String databaseName = databaseConfig.getDatabaseName();
//
//        boolean result = getAllTables(connection, databaseName).contains(tableName);
//        connection.close();
//        return result;
//    }
//

    public List<String> getAllColumns(int id, String tableName) throws SQLException {
        DatabaseMysql databaseMysql = databaseMysqlMapper.selectById(id);
        if (databaseMysql == null) {
            throw new InfoNotInDatabaseException("????????????????????????: ");
        }
        Connection connection = getConnection(databaseMysql);

        if (!getAllTables(connection, databaseMysql.getDatabaseName()).contains(tableName)) {
            throw new TableNotExistException(databaseMysql.getDatabaseName() + "????????????????????????" + tableName);
        }

        List<String> results = getAllColumnsAndTypesAndPri(connection, databaseMysql.getDatabaseName(), tableName).get(WholeVariable.COLUMN_NAME);
        connection.close();
        return results;
    }
//
//    public Boolean alertColumns(long id, String tableName, String colums, String columTypes) throws SQLException {
//        String[] columsArray = colums.split(" ");
//        String[] columTypeArray = columTypes.split(" ");
//        if (columsArray.length != columTypeArray.length) {
//            throw new IllegalArgumentException("????????????????????????????????????");
//        }
//        List<String> oldColums = getAllColumns(id, tableName);
//        Map<String, String> alertColums = new HashMap<>();
//        for (int i = 0; i < columsArray.length; i++) {
//            if (oldColums.contains(columsArray[i])) {
//                continue;
//            } else {
//                alertColums.put(columsArray[i], columTypeArray[i]);
//            }
//        }
//        MysqlDatabaseConfig databaseConfig = databaseMysqlMapper.getOne(id);
//        Connection connection = getConnection(databaseConfig);
//        if (!getAllTables(connection, databaseConfig.getDatabaseName()).contains(tableName)) {
//            throw new TableNotExistException(databaseConfig.getDatabaseName() + "????????????????????????" + tableName);
//        }
//        String sql = "ALTER TABLE " + tableName + " ADD (";
//        for (Map.Entry entry : alertColums.entrySet()) {
//            sql += entry.getKey() + " " + DataTypeConstans.getTypeByName(entry.getValue()) + ",";
//        }
//        if(sql==null){
//            return true;
//        }
//        sql = sql.substring(0, sql.lastIndexOf(","));
//        sql += ");";
//        //System.out.println("sql: "+sql);
//        PreparedStatement prst = connection.prepareStatement(sql);
//        try {
//            prst.execute();
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return false;
//    }
//
//    public Boolean batchUpdate(long id, String[] columnType,String sql,List<Object[]> batchArgs,String preOption,String aftOption) throws SQLException {
//        PreparedStatement prst;
//        PreparedStatement prstPre;
//        PreparedStatement prstAft;
//        Connection connection = getConnection(getConnectionConfig(id));
//        String sqlCopy = sql;
//        for (Object[] objects:batchArgs){
//            String tempStr = "(";
//            for (int i=0;i<objects.length;i++){
//                String type = columnType[i];
//                String addStr = "";
//                if (type.equals("String")){
//                    addStr+="\""+objects[i]+"\"";
//                }
//                else {
//                    addStr += objects[i];
//                }
//                tempStr+=addStr+",";
//            }
//            sqlCopy +=tempStr.substring(0,tempStr.lastIndexOf(","))+"),";
//        }
//
//
//        sqlCopy = sqlCopy.substring(0,sqlCopy.lastIndexOf(","));
//        sqlCopy+=";";
//        if (preOption !=null && preOption.length()>0){
//            prstPre = connection.prepareStatement(preOption);
//            prstPre.execute();
//        }
//        prst = connection.prepareStatement(sqlCopy);
//        try {
//            prst.execute();
//            if (aftOption !=null && aftOption.length() >0){
//                prstAft = connection.prepareStatement(aftOption);
//                prstAft.execute();
//            }
//            return true;
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//        return false;
//    }
//
//    /**
//     * ??????mysql?????????????????????????????????
//     * @param id
//     * @param tableName
//     * @return
//     */
//    public Map<String, List<String>> getAllColumnsAndTypesAndPri(long id, String tableName)
//            throws SQLException {
//        if (!databaseMysqlMapper.existsById(id)) {
//            throw new InfoNotInDatabaseException("????????????????????????: ");
//        }
//        MysqlDatabaseConfig databaseConfig = databaseMysqlMapper.getOne(id);
//
//        String databaseName = databaseConfig.getDatabaseName();
//        Connection connection = getConnection(databaseConfig);
//        if (!getAllTables(connection, databaseName).contains(tableName)) {
//            throw new TableNotExistException(databaseName + "????????????????????????" + tableName);
//        }
//
//        Map<String, List<String>> results = getAllColumnsAndTypesAndPri(connection, databaseName, tableName);
//        connection.close();
//        return results;
//    }

    private Map<String, List<String>> getAllColumnsAndTypesAndPri(Connection connection, String databaseName, String tableName) throws SQLException {
        ResultSet rs = null;
        PreparedStatement prst;
        String sql = "SELECT COLUMN_NAME,DATA_TYPE,COLUMN_KEY from information_schema.COLUMNS WHERE TABLE_NAME = '"
                + tableName + "' AND TABLE_SCHEMA = '" + databaseName + "';";
        List<String> columnNames = new ArrayList<>();
        List<String> types = new ArrayList<>();

        List<String> primaryKey = new ArrayList<>();
        prst = connection.prepareStatement(sql);
        rs = prst.executeQuery();
        while (rs.next()) {
            columnNames.add(rs.getString(1));
            types.add(rs.getString(2));
            primaryKey.add(rs.getString(3));

        }
        Map<String, List<String>> res = new HashMap<>();

        res.put(WholeVariable.COLUMN_NAME, columnNames);
        res.put(WholeVariable.DATA_TYPE, types);
        res.put(WholeVariable.PRIMARY_KEY, primaryKey);
        prst.close();
        return res;
    }

    public Boolean checkSql(int dataSourceId, String sql) {
        if (StringUtils.isEmpty(sql)) {
            return false;
        }
        String checkSql = sql.toUpperCase();
        if (checkSql.contains("DELETE") || checkSql.contains("DROP") || !checkSql.contains("SELECT")) {
            throw new CommonException(ResultCode.VALIDATED_FAIL, "sql???????????????????????????");
        }

        DatabaseMysql databaseMysql = databaseMysqlMapper.selectById(dataSourceId);
        try {
            Connection connection = getConnection(databaseMysql);
            PreparedStatement prst = connection.prepareStatement(sql);
            return prst.execute();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new CommonException(ResultCode.VALIDATED_FAIL, "sql??????");
        }
    }

//    /**
//     * ???mysql????????????????????????
//     *
//     * @param id
//     * @param tableName
//     * @param columnNames
//     * @param types
//     * @param primaryKey
//     * @throws SQLException
//     */
//    public void createNewTable(long id, String tableName, List<String> columnNames,
//                               List<String> types, String primaryKey) throws SQLException {
//        if (!databaseMysqlMapper.existsById(id)) {
//            throw new InfoNotInDatabaseException("????????????????????????: ");
//        }
//        MysqlDatabaseConfig databaseConfig = databaseMysqlMapper.getOne(id);
//
//        String sql = "create table if not exists " + tableName + "(";
//
//        System.out.println("size:" + columnNames.size());
//        System.out.println("types:" + types.size());
//        if (columnNames.size() != types.size()) {
//
//            throw new IllegalArgumentException("??????????????????");
//        }
//        for (int i = 0; i < columnNames.size() - 1; i++) {
//            sql += (" " + columnNames.get(i) + " " + types.get(i) + " , ");
//        }
//        sql += (" " + columnNames.get(columnNames.size() - 1) + " " + types.get(types.size() - 1));
//        if (primaryKey == null) {
//            sql += ")";
//        } else {
//            sql += ", primary key (" + primaryKey + "))";
//        }
//
//        System.out.println(sql);
//
//        Connection connection = getConnection(databaseConfig);
//        PreparedStatement prst = connection.prepareStatement(sql);
//        prst.execute();
//
//        prst.close();
//        connection.close();
//
//    }
//
//    public void createNewTable(Long databaseId,List<FieldAndType> fieldAndTypes,String tableName) throws
//            SQLException{
//        MysqlDatabaseConfig databaseConfig = getConnectionConfig(databaseId);
//        if (databaseConfig == null){
//            throw new InfoNotInDatabaseException("mysql???????????????");
//        }
//        Connection connection = getConnection(databaseConfig);
//        SqlExcuter sqlExcuter = new SqlExcuter();
//        sqlExcuter.setConnection(connection);
//        sqlExcuter.createNewTable(tableName,fieldAndTypes);
//        sqlExcuter.closeAll();
//
//    }
//
//
//
//    /**
//     * ??????mysql??????????????????
//     *
//     * @param id
//     * @return
//     */
//    public List<String> getAllViews(long id) throws SQLException {
//        if (!databaseMysqlMapper.existsById(id)) {
//            throw new InfoNotInDatabaseException("????????????????????????: ");
//        }
//        MysqlDatabaseConfig databaseConfig = databaseMysqlMapper.getOne(id);
//        Connection connection = getConnection(databaseConfig);
//        List<String> databases = getAllDatabases(connection);
//        if (!databases.contains(databaseConfig.getDatabaseName())) {
//            System.out.println("???????????????");
//            throw new DatabaseNotExistException("???????????????");
//        }
//
//        System.out.println("?????????????????????" + id + " " + databaseConfig.getDatabaseName());
//        List<String> results = getAllViews(connection, databaseConfig.getDatabaseName());
//        connection.close();
//        return results;
//    }
//
//    private List<String> getAllViews(Connection connection, String databaseName) throws SQLException {
//        ResultSet rs = null;
//        PreparedStatement prst;
//        String sql = "SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '" + databaseName + "';";
//        List<String> viewNames = new ArrayList<>();
//
//        prst = connection.prepareStatement(sql);
//        rs = prst.executeQuery();
//        while (rs.next()) {
//            viewNames.add(rs.getString(1));
//        }
//        prst.close();
//        return viewNames;
//    }
//
//    /**
//     * ??????mysql??????????????????????????????
//     *
//     * @param id
//     * @return
//     */
//    public Map<String, List<String>> getAllColumnByViews(long id,String viewName) throws SQLException {
//        if (!databaseMysqlMapper.existsById(id)) {
//            throw new InfoNotInDatabaseException("????????????????????????: ");
//        }
//        MysqlDatabaseConfig databaseConfig = databaseMysqlMapper.getOne(id);
//        Connection connection = getConnection(databaseConfig);
//        List<String> databases = getAllDatabases(connection);
//        if (!databases.contains(databaseConfig.getDatabaseName())) {
//            System.out.println("???????????????");
//            throw new DatabaseNotExistException("???????????????");
//        }
//
//        System.out.println("?????????????????????" + id + " " + databaseConfig.getDatabaseName());
//        Map<String, List<String>> results = getAllColumnByViews(connection, databaseConfig.getDatabaseName(),viewName);
//        connection.close();
//        return results;
//    }
//
//
//    private Map<String, List<String>> getAllColumnByViews(Connection connection, String databaseName, String viewName) throws SQLException {
//        ResultSet rs = null;
//        PreparedStatement prst;
//        String sql = "SELECT COLUMN_NAME,DATA_TYPE,COLUMN_KEY from information_schema.COLUMNS WHERE TABLE_NAME = '"
//                + viewName + "' AND TABLE_SCHEMA = '" + databaseName + "';";
//        List<String> columnNames = new ArrayList<>();
//        List<String> types = new ArrayList<>();
//
//        List<String> primaryKey = new ArrayList<>();
//        prst = connection.prepareStatement(sql);
//        rs = prst.executeQuery();
//        while (rs.next()) {
//            columnNames.add(rs.getString(1));
//            types.add(rs.getString(2));
//            primaryKey.add(rs.getString(3));
//
//        }
//        Map<String, List<String>> res = new HashMap<>();
//
//        res.put(WholeVariable.COLUMN_NAME, columnNames);
//        prst.close();
//        return res;
//    }
//
//    /**
//     * ??????mysql????????????????????????
//     *
//     * @param id
//     * @return
//     */
//    public Map<Map<String, String>, List<String>> getBasicsInformation(long id) throws SQLException {
//        if (!databaseMysqlMapper.existsById(id)) {
//            throw new InfoNotInDatabaseException("????????????????????????: ");
//        }
//        MysqlDatabaseConfig databaseConfig = databaseMysqlMapper.getOne(id);
//        Connection connection = getConnection(databaseConfig);
//        List<String> databases = getAllDatabases(connection);
//        if (!databases.contains(databaseConfig.getDatabaseName())) {
//            System.out.println("???????????????");
//            throw new DatabaseNotExistException("???????????????");
//        }
//
//        List<String> tableNames = getAllTables(id);
//        Map<String, String> databaseInformation = new HashMap<>();
//        databaseInformation.put("id", Long.toString(id));
//        databaseInformation.put("databaseName", databaseConfig.getDatabaseName());
//        Map<Map<String, String>, List<String>> basicsInformation = new HashMap<>();
//        basicsInformation.put(databaseInformation, tableNames);
//        return basicsInformation;
//    }
//
//    /**
//     * ??????mysql??????????????????
//     *
//     * @param id
//     * @param tableName
//     * @return
//     */
//    public List<Map<String, Object>> getDataPreview(long id, String tableName)
//            throws SQLException {
//        if (!databaseMysqlMapper.existsById(id)) {
//            throw new InfoNotInDatabaseException("????????????????????????: ");
//        }
//        MysqlDatabaseConfig databaseConfig = databaseMysqlMapper.getOne(id);
//        Connection connection = getConnection(databaseConfig);
//        if (!getAllTables(connection, databaseConfig.getDatabaseName()).contains(tableName)) {
//            throw new TableNotExistException(databaseConfig.getDatabaseName() + "????????????????????????" + tableName);
//        }
//        List<Map<String, Object>> results = getDataPreview(connection, tableName);
//        connection.close();
//        return results;
//    }
//
//    private List<Map<String, Object>> getDataPreview(Connection connection, String tableName) throws SQLException {
//        ResultSet rs = null;
//        PreparedStatement prst;
//        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
//        String sql = "SELECT * from "
//                + tableName + " limit 0,5;";
//        prst = connection.prepareStatement(sql);
//        rs = prst.executeQuery();
//        ResultSetMetaData rsmd = rs.getMetaData();
//        int colCount = rsmd.getColumnCount();
//        System.out.println(colCount);
//        List<String> colNameList = new ArrayList<String>();
//        for (int i = 0; i < colCount; i++) {
//            colNameList.add(rsmd.getColumnName(i + 1));
//        }
//        while (rs.next()) {
//            Map map = new HashMap<String, Object>();
//            for (int i = 0; i < colCount; i++) {
//                String key = colNameList.get(i);
//                Object value = rs.getString(colNameList.get(i));
//                map.put(key, value);
//            }
//            results.add(map);
//        }
//        prst.close();
//        return results;
//    }
//
//    /**
//     * mysql??????????????????
//     *
//     * @param id
//     * @return
//     */
//    public List<List<Map<String, Object>>> dataOutputInterface(long id, String tableName, String sqlString, int maxRecords)
//            throws SQLException, UnsupportedEncodingException {
//        if(!sqlString.toUpperCase().contains("SELECT")){
//            throw new SqlAntiInjectionException("sql???????????????");
//        }
//        if (!databaseMysqlMapper.existsById(id)) {
//            throw new InfoNotInDatabaseException("????????????????????????: ");
//        }
//        MysqlDatabaseConfig databaseConfig = databaseMysqlMapper.getOne(id);
//        Connection connection = getConnection(databaseConfig);
//        if (!getAllTables(connection, databaseConfig.getDatabaseName()).contains(tableName)) {
//            throw new TableNotExistException(databaseConfig.getDatabaseName() + "????????????????????????" + tableName);
//        }
//        List<List<Map<String, Object>>> results = dataOutputInterface(connection, sqlString, maxRecords);
//        connection.close();
//        return results;
//    }
//
//    private List<List<Map<String, Object>>> dataOutputInterface(Connection connection, String sqlString, int maxRecords) throws SQLException {
//        ResultSet rs = null;
//        PreparedStatement prst;
//        int index = 0;
//        List<List<Map<String, Object>>> results = new ArrayList<>();
//        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
//        String sql = sqlString;
//        prst = connection.prepareStatement(sql);
//        rs = prst.executeQuery();
//        ResultSetMetaData rsmd = rs.getMetaData();
//        int colCount = rsmd.getColumnCount();
//        List<String> colNameList = new ArrayList<String>();
//        for (int i = 0; i < colCount; i++) {
//            colNameList.add(rsmd.getColumnName(i + 1));
//        }
//        while (rs.next()) {
//            index++;
//            Map map = new HashMap<String, Object>();
//            for (int i = 0; i < colCount; i++) {
//                String key = colNameList.get(i);
//                Object value = rs.getString(colNameList.get(i));
//                map.put(key, value);
//            }
//            result.add(map);
//            if (index == maxRecords) {
//                results.add(new ArrayList<>(result));
//                result.clear();
//                index = 0;
//            }
//        }
//        if(result.size()!=0){
//            results.add(result);
//        }
//        prst.close();
//        String resultString = JSON.toJSONString(results);
//        return results;
//    }
//
//    /**
//     * ??????mysql?????????????????????-???????????????
//     *
//     * @param id
//     * @param tableName
//     * @return
//     */
//    public List<Map<String, Object>> getInterfacePreview(long id, String tableName, String sqlString)
//            throws SQLException {
//        if(!sqlString.toUpperCase().contains("SELECT")){
//            throw new SqlAntiInjectionException("????????????????????????");
//        }
//        if (!databaseMysqlMapper.existsById(id)) {
//            throw new InfoNotInDatabaseException("????????????????????????: ");
//        }
//        MysqlDatabaseConfig databaseConfig = databaseMysqlMapper.getOne(id);
//        Connection connection = getConnection(databaseConfig);
//        if (!getAllTables(connection, databaseConfig.getDatabaseName()).contains(tableName)) {
//            throw new TableNotExistException(databaseConfig.getDatabaseName() + "????????????????????????" + tableName);
//        }
//        List<Map<String, Object>> results = getInterfacePreview(connection, tableName, sqlString);
//        connection.close();
//        return results;
//    }
//
//    private List<Map<String, Object>> getInterfacePreview(Connection connection, String tableName, String sqlString) throws SQLException {
//        ResultSet rs = null;
//        PreparedStatement prst;
//        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
//        String sql = sqlString;
//        int index = 0;
//        prst = connection.prepareStatement(sql);
//        rs = prst.executeQuery();
//        ResultSetMetaData rsmd = rs.getMetaData();
//        int colCount = rsmd.getColumnCount();
//        System.out.println(colCount);
//        List<String> colNameList = new ArrayList<String>();
//        for (int i = 0; i < colCount; i++) {
//            colNameList.add(rsmd.getColumnName(i + 1));
//        }
//        while (rs.next()) {
//            index++;
//            Map map = new HashMap<String, Object>();
//            for (int i = 0; i < colCount; i++) {
//                String key = colNameList.get(i);
//                Object value = rs.getString(colNameList.get(i));
//                map.put(key, value);
//            }
//            results.add(map);
//            if (index >= 5) {
//                break;
//            }
//        }
//        prst.close();
//        return results;
//    }
//
//    /**
//     * ??????mysql?????????????????????-Json???
//     *
//     * @param id
//     * @param tableName
//     * @return
//     */
//    public String getInterfacePreviewJson(long id, String tableName, String sqlString)
//            throws SQLException {
//        String results = JSON.toJSONString(getInterfacePreview(id, tableName, sqlString));
//        return results;
//    }
//
//    private List<Map<String, Object>> getAllDataPreview(Connection connection, String tableName) throws SQLException {
//        ResultSet rs = null;
//        PreparedStatement prst;
//        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
//        String sql = "SELECT * from " + tableName + " ;";
//        prst = connection.prepareStatement(sql);
//        rs = prst.executeQuery();
//        ResultSetMetaData rsmd = rs.getMetaData();
//        int colCount = rsmd.getColumnCount();
//        System.out.println(colCount);
//        List<String> colNameList = new ArrayList<String>();
//        for (int i = 0; i < colCount; i++) {
//            colNameList.add(rsmd.getColumnName(i + 1));
//        }
//        while (rs.next()) {
//            Map map = new HashMap<String, Object>();
//            for (int i = 0; i < colCount; i++) {
//                String key = colNameList.get(i);
//                Object value = rs.getString(colNameList.get(i));
//                map.put(key, value);
//            }
//            results.add(map);
//        }
//        prst.close();
//        return results;
//    }
//
//    /**
//     * ??????????????????
//     */
//    public List<Map<String, Object>> getAllData(long id, String tableName) throws SQLException {
//        if (!databaseMysqlMapper.existsById(id)) {
//            throw new InfoNotInDatabaseException("????????????????????????: ");
//        }
//        MysqlDatabaseConfig databaseConfig = databaseMysqlMapper.findById(id).orElse(new MysqlDatabaseConfig());
//        Connection connection = getConnection(databaseConfig);
//        if (!getAllTables(connection, databaseConfig.getDatabaseName()).contains(tableName)) {
//            throw new TableNotExistException(databaseConfig.getDatabaseName() + "????????????????????????" + tableName);
//        }
//        List<Map<String, Object>> allDate = getAllDataPreview(connection, tableName);
//        return allDate;
//    }
//
//    /**
//     * ??????mysql???????????????????????????
//     * @param id
//     * @param tableName
//     * @return
//     */
//    public Map<String, List<String>> getTableStructureInfo(long id, String tableName)
//            throws SQLException {
//        if (!databaseMysqlMapper.existsById(id)) {
//            throw new InfoNotInDatabaseException("????????????????????????: ");
//        }
//        MysqlDatabaseConfig databaseConfig = databaseMysqlMapper.getOne(id);
//
//        String databaseName = databaseConfig.getDatabaseName();
//        Connection connection = getConnection(databaseConfig);
//        if (!getAllTables(connection, databaseName).contains(tableName)) {
//            throw new TableNotExistException(databaseName + "????????????????????????" + tableName);
//        }
//
//        Map<String, List<String>> results = getTableStructureInfo(connection, databaseName, tableName);
//        connection.close();
//        return results;
//    }
//    private Map<String, List<String>> getTableStructureInfo(Connection connection, String databaseName, String tableName) throws SQLException {
//        ResultSet rs = null;
//        PreparedStatement prst;
//        String sql = "SELECT " +
//                "COLUMN_NAME as ?????????," +
//                "COLUMN_TYPE as ????????????," +
//                "CHARACTER_MAXIMUM_LENGTH as ??????," +
//                "NUMERIC_PRECISION as ??????," +
//                "IS_NULLABLE as ?????????," +
//                "case when EXTRA='auto_increment' then 'YES' when EXTRA='' then 'NO' end as ????????????," +
//                "COLUMN_KEY as ??????," +
//                "COLUMN_DEFAULT as ?????????," +
//                "COLUMN_COMMENT as ?????? FROM " +
//                "information_schema.COLUMNS " +
//                "WHERE TABLE_NAME ='"+tableName+"' AND TABLE_SCHEMA = '"+databaseName+"';";
//
//        //9??????????????????
//        List<String> columnsNames = new ArrayList<>();
//        List<String> columnTypes = new ArrayList<>();
//        List<String> columnLength= new ArrayList<>();
//        List<String> precision = new ArrayList<>();
//        List<String> extra = new ArrayList<>();
//        List<String> isNullable = new ArrayList<>();
//        List<String> primaryKey = new ArrayList<>();
//        List<String> columnDefault = new ArrayList<>();
//        List<String> columnComment = new ArrayList<>();
//        prst = connection.prepareStatement(sql);
//        rs = prst.executeQuery();
//        while (rs.next()) {
//            columnsNames.add(rs.getString(1));
//            columnTypes.add(rs.getString(2));
//            columnLength.add(rs.getString(3));
//            precision.add(rs.getString(4));
//            isNullable.add(rs.getString(5));
//            extra.add(rs.getString(6));
//            primaryKey.add(rs.getString(7));
//            columnDefault.add(rs.getString(8));
//            columnComment.add(rs.getString(9));
//        }
//        Map<String, List<String>> res = new HashMap<>();
//        res.put(WholeVariable.COLUMN_NAME, columnsNames);
//        res.put(WholeVariable.COLUMN_TYPE, columnTypes);
//        res.put(WholeVariable.COLUMN_LENGTH, columnLength);
//        res.put(WholeVariable.PRIMARY_KEY, primaryKey);
//        res.put(WholeVariable.NUMERIC_PRECISION, precision);
//        res.put(WholeVariable.IS_NULLABLE, isNullable);
//        res.put(WholeVariable.COLUMN_DEFAULT, columnDefault);
//        res.put(WholeVariable.EXTRA, extra);
//        res.put(WholeVariable.COLUMN_COMMENT, columnComment);
//        prst.close();
//        return res;
//    }
}
