package com.bdilab.flinketl.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @Created with qml
 * @author:qml
 * @Date: 2019/10/22
 * @Time: 16:01
 * @Description
 */

public class WholeVariable {

    //数据库中存储的初始taskid的值 和 数据库配置的值
    public static final Long INIT_Variable_ID = (long)1;
    //数据库中存储普通任务的taskid的标识主键值
    public static final Integer INIT_ID = 1;

    public static final Integer MYSQL_ID = 2;

    public static final Integer ORACLE_ID = 3;

    public static final Integer SQLSERVER_ID = 4;

    public static final Integer HBASE_ID = 5;

    public static final Integer HDFS_ID = 6;

    public static final Integer HIVE_ID = 7;

    public static final Integer MONGODB_ID = 8;

    public static final Integer EXCEL_ID = 9;

    public static final Integer CSV_ID = 10;

    public static final Integer POSTGRESQL_ID = 11;

    public static final Integer WEBAPI_ID = 12;

    public static final Integer DAMENG_ID = 13;

    public static final Integer GBASE_ID = 14;

    public static final Integer KINGBASE_ID = 15;

    public static final Integer OSCAR_ID = 16;


    public final static String FILE_SEPARATOR = System.getProperty("file.separator");

    public final static String rootPath =System.getProperty("user.dir");

//    public static final Integer INIT_TIMING_ID = 2;
//
//    public static final Integer INIT_TRIGGER_ID = 3;

    //存储数据库名与其标识的映射
    public static final HashMap<String, Integer> DATABASE_TYPE = new HashMap();

    public static final HashMap<String, Integer> COMPONENT_TYPE = new HashMap<>();

    //数据库类型标识
    public static final String MYSQL = "mysql";
    public static final String ORACLE = "oracle";
    public static final String SQLSERVER = "sqlserver";
    public static final String HBASE = "hbase";
    public static final String HDFS = "hdfs";
    public static final String KAFKA = "kafka";
    public static final String HIVE = "hive";
    public static final String MONGODB = "mongoDB";
    public static final String EXCEL = "excel";
    public static final String CSV = "csv";

    public static final String POSTGRESQL = "postgresql";

    public static final String WEBAPI = "web api";

    public static final String DAMENG = "DaMeng";

    public static final String GBASE = "Gbase";

    public static final String KINGBASE = "Kingbase";

    public static final String OSCAR = "Oscar";


    //源数据库标识
    public static final int SOURCE = 0;
    //目标数据库标识
    public static final int TARGET = 1;
    //数据库作为目标还是源的位数
    public static final int SYMBOLPE_PLACES = 10;

    //数据库类型位数
    public static final int DATABASE_TYPE_PLACES = 100;

    //组件类型位数
    public static final int COMPONENT_TYPE_PLACES = 100;


    //组件类型标识
    //输入
    public static final String INPUT = "input";
    //输出
    public static final String OUTPUT = "output";
    //字段映射
    public static final String FIELD_MAP = "fieldMap";
    //字段过滤
    public static final String FIELD_FILTER = "fieldFilter";

    //字段替换
    public static final String FIELD_REPLACE = "fieldReplace";
    //字段合并
    public static final String MERGE_JOIN = "mergeJoin";

    //字段计算组件
    public static final String FIELD_CALCULATOR = "fieldCalaulator";

    //字段分割
    public static final String FIELD_SPLIT = "fieldSplit";

    // 空步骤
    public static final String FIELD_EMPTY = "";


    //任务状态
    public static final int NO_BEGIN= 0;
    public static final int RUNNING = 1;
    public static final int PAUSE = 2;
    public static final int STOP = 3;
    public static final int END = 4;
    public static final int FAIL = 5;
    public static HashMap taskStatus = new HashMap();

    //接口服务的操作类型
    public static final int SUCCESS= 0;
    public static final int FAILED = 1;
    public static final int ENABLE= 2;
    public static final int DISABLE = 3;

    public static final int TASK_TYPE_PLACES = 10;

    //获取各个节点资源使用情况
    //所要访问的端口号
    public static final String GET_RESOURCE_PORT = "8090";
    //url头部
    public static final String URL_HEAD = "http://";
    //所要访问的http接口
    public static final String URL_END = "/api/getResourceDefault";

    public static final String SHH_URL = "http://localhost:8091/api/shell/run";

//    public static final String SCP_SHELL;/* = "sh /home/project/ETL4.0/shell/scp_file.sh ";*/
//
//    public static final String RESTART_SHELL;/* = "sh /home/project/ETL4.0/shell/restartSlaveAll.sh ";*/

    static {
        DATABASE_TYPE.put(MYSQL, 10);
        DATABASE_TYPE.put(ORACLE, 11);
        DATABASE_TYPE.put(SQLSERVER, 12);
        DATABASE_TYPE.put(MONGODB, 13);
        DATABASE_TYPE.put(POSTGRESQL,14);
        DATABASE_TYPE.put(DAMENG,15);
        DATABASE_TYPE.put(GBASE,16);
        DATABASE_TYPE.put(KINGBASE,17);
        DATABASE_TYPE.put(OSCAR,18);

        DATABASE_TYPE.put(EXCEL, 20);
        DATABASE_TYPE.put(CSV, 21);
        DATABASE_TYPE.put(WEBAPI,22);

        DATABASE_TYPE.put(HBASE, 30);
        DATABASE_TYPE.put(HDFS,31);
        DATABASE_TYPE.put(HIVE, 32);

        DATABASE_TYPE.put(KAFKA, 40);
//        DATABASE_TYPE.put(HDFS,20);
//        DATABASE_TYPE.put(MONGODB,40);

        COMPONENT_TYPE.put(INPUT, 10);
        COMPONENT_TYPE.put(OUTPUT, 11);
        COMPONENT_TYPE.put(FIELD_MAP, 12);
        COMPONENT_TYPE.put(FIELD_FILTER, 13);
        COMPONENT_TYPE.put(FIELD_REPLACE, 14);
        COMPONENT_TYPE.put(MERGE_JOIN,15);
        COMPONENT_TYPE.put(FIELD_SPLIT,16);
        COMPONENT_TYPE.put(FIELD_CALCULATOR, 17);

        taskStatus.put(NO_BEGIN, "未开始");
        taskStatus.put(RUNNING, "进行中");
        taskStatus.put(PAUSE, "暂停中");
        taskStatus.put(STOP, "已停止");
        taskStatus.put(END, "执行成功");
        taskStatus.put(FAIL, "执行失败");

//        Properties properties = PropertiesUtil.loadProperties(rootPath + FILE_SEPARATOR
//                + "conf"+ FILE_SEPARATOR +"path.properties");
//        SCP_SHELL = "sh " + properties.getProperty("scp.shell") + " ";
//        RESTART_SHELL = "sh " + properties.getProperty("restart.shell") + " ";

    }

    //表结构信息

    public static final String COLUMN_NAME = "column_names";
    public static final String COLUMN_TYPE = "column_types";
    public static final String COLUMN_LENGTH = "column_length";
    public static final String NUMERIC_PRECISION = "numeric_precision";
    public static final String IS_NULLABLE = "is_nullable";
    public static final String EXTRA = "extra";
    public static final String PRIMARY_KEY = "primary_key";
    public static final String DATA_TYPE = "data_keys";
    public static final String COLUMN_DEFAULT = "column_default";
    public static final String COLUMN_COMMENT = "column_comment";

    // 字段类型
    public static final String INT = "int";
    public static final String VARCHAR = "varchar";
    public static final String DOUBLE = "double";
    public static final String FLOAT = "float";
    public static final String LONG = "long";
    public static final String DATE = "date";

    //Mysql的时间类型
    //date 2021-10-19
    public static final String MYSQL_DATE="date";
    //datetime 2021-10-19 00:00:00
    public static final String MYSQL_DATETIME="datetime";
    //time 00:00:00
    public static final String MYSQL_TIME="time";
    //timestamp 2021-10-11 00:00:00
    public static final String MYSQL_TIMESTAMP="timestamp";

    public static final String ORACLE_NUMBER = "number";
    public static final String ORACLE_NVARCHAR2 = "nvarchar2";

    //Hive的数据类型
    public static final String HIVE_STRING = "string";
    public static final String HIVE_INT="int";

    public static final String HIVE_DATE="date";

    //JDBC驱动名
    public static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";
    public static final String ORACLE_DRIVER_NAME = "oracle.jdbc.driver.OracleDriver";
    public static final String SQLSERVER_DRIVER_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

    public static final List<String> NUMBERS = Arrays.asList(INT, DOUBLE, FLOAT, LONG, ORACLE_NUMBER, HIVE_INT);
}
