package com.bdilab.flinketl.utils;

import com.bdilab.flinketl.entity.DatabaseSqlserver;
import com.bdilab.flinketl.mapper.DatabaseSqlServerMapper;
import com.bdilab.flinketl.utils.exception.DatabaseNotExistException;
import com.bdilab.flinketl.utils.exception.InfoNotInDatabaseException;
import com.bdilab.flinketl.utils.exception.TableNotExistException;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class SqlServerClient {
    private static final String SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String DEFAULT_DATABASE = "master";

    @Resource
    private DatabaseSqlServerMapper databaseConfigMapper;


    public Connection getConnection(DatabaseSqlserver databaseSqlServer) throws SQLException {
        Connection connection=null;
        try{
            Class.forName(SQLSERVER_DRIVER);
            String databaseName = (databaseSqlServer.getDatabaseName()==null)?DEFAULT_DATABASE:databaseSqlServer.getDatabaseName();
            String url="jdbc:sqlserver://"+databaseSqlServer.getHostname()+":"+databaseSqlServer.getPort()+";DatabaseName="+databaseName;
            System.out.println(SQLSERVER_DRIVER);
            System.out.println(url);
            connection= DriverManager.getConnection(url,databaseSqlServer.getUsername(),databaseSqlServer.getPassword());
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        }
        return connection;
    }
    public String testConnection(DatabaseSqlserver databaseSqlServer) throws SQLException {
        Connection connection = getConnection(databaseSqlServer);
        if (connection != null) {
            connection.close();
            return "success";
        }
        return "fail";
    }

    public List<String> getAllDatabases(DatabaseSqlserver databaseSqlServer) throws SQLException {
        Connection connection=getConnection(databaseSqlServer);
        List<String> results = getAllDatabases(connection);
        connection.close();
        return results;
    }

    private List<String> getAllDatabases(Connection connection) throws SQLException {
        ResultSet res = null;
        PreparedStatement statement;
        List<String> databaseNames = new ArrayList<>();
        statement = connection.prepareStatement("SELECT NAME fROM Master..SysDatabases ORDER BY Name");
        res = statement.executeQuery();
        while (res.next()) {
            databaseNames.add(res.getString(1));
        }
        statement.close();
        return databaseNames;
    }

    public List<String> getAllTables(int id) throws SQLException {
        DatabaseSqlserver databaseConfig = databaseConfigMapper.selectById(id);
        if (databaseConfig == null) {
            throw new InfoNotInDatabaseException("无法获取该数据库: ");
        }
        Connection connection = getConnection(databaseConfig);
        List<String> databases = getAllDatabases(connection);
        if (!databases.contains(databaseConfig.getDatabaseName())) {
            System.out.println("不存在该数据库");
            throw new DatabaseNotExistException("不存在该数据库");
        }

        List<String> results = getAllTables(connection, databaseConfig.getDatabaseName());
        connection.close();
        return results;
    }

    private List<String> getAllTables(Connection connection, String databaseName) throws SQLException {
        ResultSet rs = null;
        PreparedStatement prst;
        //目前这个主要查看所有数据库的用户表
        // XType='U'    :表示所有用户表;
        // XType='S'    :表示所有系统表;
        String sql = "SELECT NAME FROM " + databaseName + "..SysObjects Where XType=\'U\' ORDER BY Name;";
        List<String> tableNames = new ArrayList<>();

        prst = connection.prepareStatement(sql);
        rs = prst.executeQuery();
        while (rs.next()) {
            tableNames.add(rs.getString(1));
        }
        prst.close();
        return tableNames;
    }

    public List<String> getAllColumns(int id, String tableName) throws SQLException {
        DatabaseSqlserver databaseConfig = databaseConfigMapper.selectById(id);
        if (databaseConfig == null) {
            throw new InfoNotInDatabaseException("无法获取该数据库: ");
        }
        Connection connection = getConnection(databaseConfig);

        if (!getAllTables(connection, databaseConfig.getDatabaseName()).contains(tableName)) {
            throw new TableNotExistException(databaseConfig.getDatabaseName() + "数据库中不存在表" + tableName);
        }

        List<String> results = getAllColumnsAndTypesAndPri(connection, databaseConfig.getDatabaseName(), tableName).get(WholeVariable.COLUMN_NAME);
        connection.close();
        return results;
    }

    private Map<String, List<String>> getAllColumnsAndTypesAndPri(Connection connection, String databaseName, String tableName) throws SQLException {
        ResultSet rs = null;
        PreparedStatement prst;
        String sql = "select a.name,b.name,c.type from "+databaseName+".sys.columns a inner join "
                +databaseName+".sys.types b on a.user_type_id=b.user_type_id left join "
                +databaseName+".sys.indexes i on a.object_id=i.object_id and a.column_id= i.index_id left join "
                +databaseName+".sys.key_constraints c on c.unique_index_id=i.index_id where a.object_id=Object_Id('"
                + tableName +"');";
        //"SELECT name FROM syscolumns WHERE id=Object_Id('" + tableName +"');" 该sql语句指获取列名字
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

}
