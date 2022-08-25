package com.bdilab.flinketl.utils;

import com.bdilab.flinketl.entity.DatabaseHive;
import com.bdilab.flinketl.entity.vo.TableStructureVO;
import com.bdilab.flinketl.mapper.DatabaseHiveMapper;
import com.bdilab.flinketl.utils.exception.DatabaseNotExistException;
import com.bdilab.flinketl.utils.exception.InfoNotInDatabaseException;
import com.bdilab.flinketl.utils.exception.TableNotExistException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class HiveClient {
    @Resource
    private DatabaseHiveMapper databaseHiveMapper;

    private Connection getConnection(DatabaseHive databaseHive) {
        Connection connection = null;
        /*hiverserver 版本使用此驱动*/
        //String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
        /*hiverserver2 版本使用此驱动*/
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        try {
            Class.forName(driverName);
            String databaseName = (databaseHive.getDatabaseName() == null) ? "default" : databaseHive.getDatabaseName();
            String url = "jdbc:hive2://" + databaseHive.getHostname() + ":" + databaseHive.getPort() + "/" + databaseName
                    + "?connectTimeout=30000";
            connection = DriverManager.getConnection(url, databaseHive.getUsername(), databaseHive.getPassword());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    public String testConnection(DatabaseHive databaseHive) throws SQLException {
        Connection connection = getConnection(databaseHive);
        if (connection != null) {
            connection.close();
            return "success";
        }
        return "fail";
    }

    public List<String> getAllDatabases(DatabaseHive databaseHive) throws SQLException {
        Connection connection = getConnection(databaseHive);
        List<String> results = getAllDatabases(connection);

        connection.close();
        return results;
    }

    private List<String> getAllDatabases(Connection connection) throws SQLException {
        List<String> databaseNames = new ArrayList<>();
        PreparedStatement prst = connection.prepareStatement("SHOW DATABASES");
        ResultSet rs = prst.executeQuery();
        while (rs.next()) {
            databaseNames.add(rs.getString(1));
        }
        prst.close();
        return databaseNames;
    }

    public List<String> getAllTables(int id) throws SQLException {
        DatabaseHive databaseHive = databaseHiveMapper.selectById(id);
        if (databaseHive == null) {
            throw new InfoNotInDatabaseException("无法获取该数据库: ");
        }
        Connection connection = getConnection(databaseHive);
        List<String> databases = getAllDatabases(connection);
        if (!databases.contains(databaseHive.getDatabaseName())) {
            System.out.println("无该数据库");
            throw new DatabaseNotExistException("无该数据库");
        }

        List<String> results = getAllTables(connection, databaseHive.getDatabaseName());
        connection.close();
        return results;
    }

    private List<String> getAllTables(Connection connection, String databaseName) throws SQLException {
        List<String> tablesNames = new ArrayList<>();
        PreparedStatement prst = connection.prepareStatement("SHOW TABLES");
        ResultSet rs = prst.executeQuery();
        while (rs.next()) {
            tablesNames.add(rs.getString(1));
        }
        prst.close();
        return tablesNames;
    }


    public List<TableStructureVO> getTableStructureInfo(long id, String tableName)
            throws SQLException {
        DatabaseHive databaseHive = databaseHiveMapper.selectById(id);
        if (databaseHive == null) {
            throw new InfoNotInDatabaseException("无法获取该数据库: ");
        }
        String databaseName = databaseHive.getDatabaseName();
        Connection connection = getConnection(databaseHive);
        if (!getAllTables(connection,databaseName).contains(tableName)) {
            throw new TableNotExistException(databaseName + "数据库中不存在表" + tableName);
        }
        List<TableStructureVO> results = getTableStructureInfo(connection, databaseName,tableName);
        connection.close();
        return results;
    }

    private List<TableStructureVO> getTableStructureInfo(Connection connection,String databaseName, String tableName) throws SQLException {
        ResultSet rs = null;
        PreparedStatement prst;
        String sql = "desc " +databaseName+"."+tableName;

        List<TableStructureVO> tableStructure = new ArrayList<>();
        prst = connection.prepareStatement(sql);
        rs = prst.executeQuery();
        while (rs.next()) {
            TableStructureVO vo = new TableStructureVO();
            vo.setColumnName(rs.getString(1));
            vo.setColumnType(rs.getString(2));
            vo.setColumnComment(rs.getString(3));
            tableStructure.add(vo);
        }
        prst.close();
        return tableStructure;
    }

    public List<String> getAllColumns(int id, String tableName) throws SQLException {
        DatabaseHive databaseHive = databaseHiveMapper.selectById(id);
        if (databaseHive == null) {
            throw new InfoNotInDatabaseException("无法获取该数据库: ");
        }
        Connection connection = getConnection(databaseHive);

        if (!getAllTables(connection, databaseHive.getDatabaseName()).contains(tableName)) {
            throw new TableNotExistException(databaseHive.getDatabaseName() + "数据库中不存在表" + tableName);
        }

        List<String> results = getAllColumnsAndTypesAndPri(connection, databaseHive.getDatabaseName(), tableName).get(WholeVariable.COLUMN_NAME);
        connection.close();
        return results;
    }

    private Map<String, List<String>> getAllColumnsAndTypesAndPri(Connection connection, String databaseName, String tableName) throws SQLException {
        Map<String, List<String>> result = new HashMap<>();
        List<String> columnNames = new ArrayList<>();
        List<String> types = new ArrayList<>();
        String sql = "describe " + databaseName + "." + tableName;

        PreparedStatement prst = connection.prepareStatement(sql);
        ResultSet rs = prst.executeQuery();
        while (rs.next()) {
            columnNames.add(rs.getString(1));
            types.add(rs.getString(2));
        }
        result.put(WholeVariable.COLUMN_NAME, columnNames);
        result.put(WholeVariable.COLUMN_TYPE, types);
        prst.close();
        return result;
    }

    public List<Map<String, Object>> getDataPreview(int id, String tableName) throws SQLException {
        DatabaseHive databaseHive = databaseHiveMapper.selectById(id);
        if (databaseHive == null) {
            throw new InfoNotInDatabaseException("无法获取该数据库: ");
        }
        String databaseName = databaseHive.getDatabaseName();
        Connection connection = getConnection(databaseHive);
        if (!getAllTables(connection,databaseHive.getDatabaseName()).contains(tableName)) {
            throw new TableNotExistException(databaseName + "数据库中不存在表" + tableName);
        }
        List<Map<String, Object>> results = getDataPreview(connection, tableName);
        connection.close();
        return results;
    }
    private List<Map<String, Object>> getDataPreview(Connection connection, String tableName) throws SQLException {
        ResultSet rs = null;
        PreparedStatement prst;
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        String sql = "SELECT * FROM "
                + tableName ;
        //sql +=" limit 5";
        prst = connection.prepareStatement(sql);
        rs = prst.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();
        int colCount = rsmd.getColumnCount();
        System.out.println(colCount);
        List<String> colNameList = new ArrayList<String>();
        for (int i = 0; i < colCount; i++) {
            colNameList.add(rsmd.getColumnName(i + 1));
        }
        while (rs.next()) {
            Map map = new HashMap<String, Object>();
            for (int i = 0; i < colCount; i++) {
                String key = colNameList.get(i);
                Object value = rs.getString(colNameList.get(i));
                map.put(key, value);
            }
            results.add(map);
        }
        Map m=new HashMap<String,Object>();
        m.put("count",results.size());
        results.add(m);
        prst.close();
        return results;
    }
    public String createTable(DatabaseHive databaseHive, String sql) throws SQLException {
        Connection connection = getConnection(databaseHive);
        Boolean flag=false;
        Statement statement = connection.createStatement();
        statement.execute(sql);
        flag=true;
        connection.close();
        if(flag){
            return "create success";
        }else{
            return "create failed";
        }

    }

}
