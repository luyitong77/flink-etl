package com.bdilab.flinketl.utils;

import com.bdilab.flinketl.entity.DatabaseMysql;
import com.bdilab.flinketl.entity.DatabaseOracle;
import com.bdilab.flinketl.mapper.DatabaseOracleMapper;
import com.bdilab.flinketl.utils.common.entity.ResultCode;
import com.bdilab.flinketl.utils.common.exception.CommonException;
import com.bdilab.flinketl.utils.exception.InfoNotInDatabaseException;
import com.bdilab.flinketl.utils.exception.TableNotExistException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Author: ljw
 * @Description:
 * @Date: create in 2021/7/27
 */
@Slf4j
@Component
public class OracleClient {

    private static final String ORACLE_DRIVER = "oracle.jdbc.OracleDriver";
    public static final String COLUMN_NAME = "column_names";
    public static final String COLUMN_TYPE = "column_types";

    private static final String DEFAULT_DATABASE = "orcl";

    @Resource
    private DatabaseOracleMapper databaseOracleMapper;


    /**
     * Oracle建立连接
     * @param databaseOracle
     * @return Connection
     * @throws SQLException
     */
    public Connection getConnection(DatabaseOracle databaseOracle) throws SQLException {
        Connection connection = null;
        if (databaseOracle.getDatabaseName() == null){
            throw new IllegalArgumentException("缺少数据库名");
        }
        try{
            Class.forName(ORACLE_DRIVER);
            String url = null;
            String databaseName = (databaseOracle.getDatabaseName() == null) ?
                    DEFAULT_DATABASE : databaseOracle.getDatabaseName();
            if (databaseOracle.getIsServiceName() == 1){
                url = "jdbc:oracle:thin:" + databaseOracle.getHostname() + ":" +
                        databaseOracle.getPort() + "/" + databaseName;
            }else {
                url = "jdbc:oracle:thin:" + databaseOracle.getHostname() + ":" +
                        databaseOracle.getPort() + ":" + databaseName;
            }
            connection = DriverManager.getConnection(url, databaseOracle.getUsername(),
                    databaseOracle.getPassword());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 根据id获取oracle中表连接配置
     * @param databaseId
     * @return DatabaseOracle
     */
    private DatabaseOracle getDatabaseOracle(long databaseId){
        return databaseOracleMapper.selectById(databaseId);
    }

    /**
     * 根据id,Oracle建立连接
     * @param databaseId
     * @return Connection
     * @throws SQLException
     */
    public Connection getConnection(long databaseId) throws SQLException {
        DatabaseOracle databaseOracle = getDatabaseOracle(databaseId);
        return getConnection(databaseOracle);
    }

    /**
     * Oracle测试连接
     * @param databaseOracle
     * @return
     * @throws SQLException
     */
    public String testConnection(DatabaseOracle databaseOracle) throws SQLException {
        Connection connection = getConnection(databaseOracle);
        if (connection != null) {
            connection.close();
            return "success";
        }
        return "fail";
    }

//    public OracleDatabaseConfig getConnectionConfig(long databaseId){
//        if (!databaseRepository.existsById(databaseId)){
//            return null;
//        }
//        return databaseRepository.getOne(databaseId);
//    }
//
//    public long saveDatabaseConfig(OracleDatabaseConfig oracleDatabaseConfig){
//        long databaseId = generateTaskIdAndDataBaseIdAndComponentId.generateDatabaseId(WholeVariable.ORACLE);
//        if (databaseId < 1){
//            return -1;
//        }
//        oracleDatabaseConfig.setId(databaseId);
//        databaseRepository.save(oracleDatabaseConfig);
//        return databaseId;
//    }
//
//    public boolean editDatabaseConfig(OracleDatabaseConfig databaseConfig){
//        if (!databaseRepository.existsById(databaseConfig.getId())){
//            throw new InfoNotInDatabaseException("数据库中不存在该信息");
//        }
//        databaseRepository.save(databaseConfig);
//        return true;
//    }
//
//    public boolean deleteConnection(long databaseId){
//        if (!databaseRepository.existsById(databaseId)){
//            return true;
//        }
//        databaseRepository.deleteById(databaseId);
//        return true;
//    }
//
//    public List<FieldAndType> getFieldAndTypeFromSql(long databaseId, String sql)throws SQLException{
//        OracleDatabaseConfig databaseConfig = getConnectionConfig(databaseId);
//        if (databaseConfig == null){
//            throw new InfoNotInDatabaseException("oracle配置不存在");
//        }
//        Connection connection = getConnection(databaseConfig);
//        List<FieldAndType> results = ParseSql.getSqlFields(connection,sql,WholeVariable.ORACLE);
//        connection.close();
//        return results;
//    }
//
//    public Map<String,String> getSingleRowFromDatabase(long databaseId, String sql) throws SQLException{
//        OracleDatabaseConfig databaseConfig = getConnectionConfig(databaseId);
//        if (databaseConfig == null) {
//            throw new InfoNotInDatabaseException("oracle配置不存在");
//        }
//        System.out.println("sql: " + sql);
//        Connection connection = getConnection(databaseConfig);
//        SqlExcuter sqlExcuter = new SqlExcuter();
//        sqlExcuter.setConnection(connection);
//        Map<String,String> results = sqlExcuter.executeQuerySingleRow(sql);
//        sqlExcuter.closeAll();
//        return results;
//    }

    /**
     * sys
     * Oracle 返回所有的'数据库(Schemas)'
     * @param databaseOracle
     * @return List<String>
     * @throws SQLException
     */
    public List<String> getALlSchemas(DatabaseOracle databaseOracle) throws SQLException{
        Connection connection = getConnection(databaseOracle);

//        List<String> results = ParseSql.getAllSchemas(connection);
        List<String> results = new ArrayList<>();
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet resultSet = databaseMetaData.getSchemas();

//        getFromResultSet(resultSet,results,"TABLE_SCHEM");
        while (resultSet.next()){
            results.add(resultSet.getString("TABLE_SCHEM"));
            System.out.println(resultSet.getString("TABLE_SCHEM"));
        }

        connection.close();
        return results;
    }


// --------------------------------------get---------------------------------
// --------------------------------------all---------------------------------
// -------------------------------------table---------------------------------

    private List<String> getAllTables(Connection connection) throws SQLException{
        ResultSet rs = null;
        PreparedStatement prst;
        List<String> tableNames = new ArrayList<>();

        prst = connection.prepareStatement("SELECT TABLE_NAME FROM USER_TABLES");
        rs = prst.executeQuery();
        while (rs.next()){
            tableNames.add(rs.getString(1));
        }
        return tableNames;
    }

    /**
     * Oracle 返回所有的表
     * @param databaseOracle
     * @return List<String>
     * @throws SQLException
     */
    public List<String> getAllTables(DatabaseOracle databaseOracle) throws SQLException{
        Connection connection = getConnection(databaseOracle);
        List<String> results = getAllTables(connection);
        connection.close();
        return results;
    }

    /**
     * 根据Oracle配置的id 返回所有的表
     * @param  id
     * @return List<String>
     * @throws SQLException
     */
    public List<String> getAllTables(long id) throws SQLException{
        DatabaseOracle databaseOracle = getDatabaseOracle(id);
        Connection connection = getConnection(databaseOracle);
        List<String> results = getAllTables(databaseOracle);
        connection.close();
        return results;
    }



    /**
     * 获取对应模式下的所有表
     * @param databaseId
     * @param userName
     * @return
     * @throws SQLException
     */
    public List<String> getAllTables(long databaseId,String userName) throws SQLException{
        DatabaseOracle databaseOracle = getDatabaseOracle(databaseId);
        Connection connection = getConnection(databaseOracle);

        List<String> results = new ArrayList<>();
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet resultSet = databaseMetaData.getTables(null,userName.toUpperCase(),
                "%",new String[]{"TABLE"});

        //getFromResultSet(resultSet,results,"TABLE_NAME");
        while (resultSet.next()){
            results.add(resultSet.getString("TABLE_NAME"));
        }
        resultSet.close();
        connection.close();
        return results;
    }


    /**
     * 判断表格是否存在
     * @param id
     * @return Boolean
     * @throws SQLException
     */
    public Boolean containsTable(long id , String tableName) throws SQLException{
        return getAllTables(id).contains(tableName.toUpperCase());
    }
    /**
     * 判断表格是否存在
     * @param databaseOracle
     * @return Boolean
     * @throws SQLException
     */
    public Boolean containsTable(DatabaseOracle databaseOracle , String tableName) throws SQLException{
        return getAllTables(databaseOracle).contains(tableName.toUpperCase());
    }

    /**
     * 返回所有的列
     * @param id
     * @return Boolean
     * @throws SQLException
     */
    public List<String> getAllColumns(long id ,String tableName) throws SQLException{

        DatabaseOracle databaseOracle = databaseOracleMapper.selectById(id);
        if (databaseOracle == null) {
            throw new InfoNotInDatabaseException("无法获取该数据库: ");
        }
        Connection connection = getConnection(databaseOracle);

        if (!containsTable(databaseOracle, tableName)) {
            throw new TableNotExistException(databaseOracle.getDatabaseName() + "数据库中不存在表" + tableName);
        }

        List<String> results = getAllColumnsAndTypes(connection,tableName).get(WholeVariable.COLUMN_NAME);
        connection.close();
        return results;
    }

    /**
     * 获取oracle指定表列名和类型
     * @param id
     * @param tableName
     * @return
     */
    public Map<String,List<String>> getAllColumnsNamesAndTypes(long id,String tableName) throws SQLException{
        DatabaseOracle databaseOracle = databaseOracleMapper.selectById(id);
        if (databaseOracle == null){
            throw new InfoNotInDatabaseException("数据库中无该信息");
        }
        Connection connection = getConnection(databaseOracle);
        Map<String,List<String>> results = getAllColumnsAndTypes(connection,tableName);
        connection.close();
        return results;
    }
    private Map<String,List<String>> getAllColumnsAndTypes(Connection connection,String tableName) throws SQLException{
        ResultSet rs = null;
        PreparedStatement prst;

        List<String> columnsNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();

        prst = connection.prepareStatement("SELECT COLUMN_NAME,DATA_TYPE FROM user_tab_columns WHERE table_name = '"
                + tableName.toUpperCase() + "'");
        rs = prst.executeQuery();
        while (rs.next()){
            columnsNames.add(rs.getString(1));
            columnTypes.add(rs.getString(2));
        }
        Map<String,List<String>> res = new HashMap<>();
        res.put(WholeVariable.COLUMN_NAME,columnsNames);
        res.put(WholeVariable.COLUMN_TYPE,columnTypes);
        prst.close();
        return res;
    }


    public boolean checkSql(String sql,Long databaseId) {
        if (StringUtils.isEmpty(sql)) {
            return false;
        }
        sql = sql.toUpperCase();
        if (sql.contains("DELETE") || sql.contains("DROP") || !sql.contains("SELECT")) {
            throw new CommonException(ResultCode.VALIDATED_FAIL, "sql语句只能为查询语句");
        }

        DatabaseOracle databaseOracle = databaseOracleMapper.selectById(databaseId);
        try {
            Connection connection = getConnection(databaseOracle);
            PreparedStatement prst = connection.prepareStatement(sql);
            return prst.execute();
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            throw new CommonException(ResultCode.VALIDATED_FAIL, "sql错误");
        }
    }

}
