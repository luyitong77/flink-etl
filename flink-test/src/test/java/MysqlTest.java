import com.bdilab.FlinkETLApplication;
import com.bdilab.demo.examples.pojo.Student;
import com.bdilab.flinketl.utils.kafka.SerializationUtil;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * @author hcyong
 * @date 2021/7/13
 */
@RunWith(SpringJUnit4ClassRunner.class)
@EnableAutoConfiguration
@SpringBootTest(classes = FlinkETLApplication.class)
public class MysqlTest {
    private final static String DRIVER = "com.mysql.jdbc.Driver";
    private final static String URL = "jdbc:mysql://192.168.0.239:3306/test";
    private final static String USERNAME = "root";
    private final static String PASSWORD = "123123";
    private static Connection conn = null;

//    @Autowired
//    UserInfoMapper userInfoMapper;
//    @Autowired
//    StudentService studentService;

    private static Connection getConnection() {
        if (conn == null) {
            try {
                Class.forName(DRIVER);
                conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

//    @Test
//    public void findUsername() {
////        System.out.println(userInfoMapper.insert(UserInfo.builder().username("hcyong").password("123123").salt("hcyong").build()));
//        System.out.println(userInfoMapper.findUserInfoByUsername("hcyong"));
//    }

    @Test
    public void insertStudent() {
        String[] provinces = {
                "HeBei", "ShanXi", "LiaoNing", "JiLin", "HeiLongJiang", "JiangSu", "ZheJiang",
                "AnHui", "FuJian", "JiangXi", "ShanDong", "HeNan", "HuBei", "HuNan",
                "GuangDong", "HaiNan", "SiChuan", "GuiZhou", "YunNan", "ShaanXi", "GanSu",
                "QingHai", "TaiWan", "NeiMengGu", "GuangXi", "XiZang", "NingXia", "XinJiang",
                "BeiJing", "TianJin", "ShangHai", "ChongQing", "XiangGang", "AoMen"
        };
        int num = 1000000;
        List<Student> list = new ArrayList<>();
        Random random = new Random();
        for (int i=0; i<num; ++i) {
            System.out.println(i);
            list.add(Student.builder()
                    .name(generateRandomString())
                    .password(generateRandomString())
                    .age(random.nextInt(88))
                    .father(generateRandomString())
                    .mother(generateRandomString())
                    .address(generateRandomString())
                    .province(provinces[random.nextInt(34)])
//                    .birthday()
                    .grade(random.nextInt(12))
                    .chinese(random.nextInt(101))
                    .math(random.nextInt(101))
                    .english(random.nextInt(101))
                    .physics(random.nextInt(101))
                    .chemistry(random.nextInt(101))
                    .biology(random.nextInt(101))
                    .politics(random.nextInt(101))
                    .history(random.nextInt(101))
                    .geography(random.nextInt(101))
                    .pe(random.nextInt(101))
                    .art(random.nextInt(101))
                    .music(random.nextInt(101))
                    .build());
        }
//        studentService.saveBatch(list);
    }

    @Test
    public void insertScore() throws SQLException {
        int num = 100000;
        Random random = new Random();
        Connection connection = getConnection();
        connection.setAutoCommit(false);
        String preSql = "insert into score(score, name) values ";
        PreparedStatement pst = connection.prepareStatement(preSql);
        for (int i=0; i<100; ++i) {
            StringBuilder sb = new StringBuilder();
            for (int j=0; j<num/100; ++j) {
                sb.append("('")
                        .append(random.nextInt(100)).append(",").append(random.nextInt(100)).append(",").append(random.nextInt(100)).append("',")
                        .append("'").append(generateRandomString()).append("'")
                        .append("),");
            }
            String sql = preSql + sb.substring(0, sb.length()-1);
            pst.addBatch(sql);
            pst.executeBatch();
            connection.commit();
        }
    }

    String generateRandomString() {
        String str = "aAbBcCdDeEfFgGhHiIjJkKlLmMnNoOpPqQrRsStTuUvVwWxXyYzZ0123456789";
        Random random = new Random();
        StringBuffer stringBuffer = new StringBuffer();
        //确定字符串长度
        int stringLength = (int) (Math.random()*10);
        for (int j = 0; j < stringLength; j++) {
            int index = random.nextInt(str.length());
            char c = str.charAt(index);
            stringBuffer.append(c);
        }
        //将StringBuffer转换为String类型的字符串
        return stringBuffer.toString();
    }

    @Test
    public void test(){
        String driverClassName = "oracle.jdbc.OracleDriver";
        String url = "jdbc:oracle:thin:@localhost:1521/orcl";
        String username = "sys as sysdba";
        String password = "1234";

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(driverClassName);
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        //设置连接池的一些参数
        dataSource.setInitialSize(2);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            System.out.println("创建连接池：" + con);
            PreparedStatement prst = con.prepareStatement("insert into score_copy1(\"sex\", \"score1\", \"score2\", \"score3\", \"id\", \"name\") values ('test', 9, 9, 9, 999, 'test')");
            prst.executeQuery();
        } catch (Exception e) {
            System.out.println("-----------oracle get connection / create pool has exception , msg = " + e.getMessage());
        }
    }

    @Test
    public void testAutoConfig() throws Exception{
        Connection connection = getConnection();
        String preSql = "select * from student_copy2;";
        PreparedStatement pst = connection.prepareStatement(preSql);
        ResultSet rs = pst.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i=1; i<=columnCount; ++i) {
            System.out.println(metaData.getColumnName(i));
            System.out.println(metaData.getColumnTypeName(i));
            System.out.println(metaData.getColumnLabel(i));
        }
    }

    @Test
    public void ttt(){
        int temp = 1;
        String rawUrl = temp == 1 ? "jdbc:oracle:thin:%s:%d/%s" : "jdbc:oracle:thin:%s:%d:%s";

        String hostname = "192.168.0.236";
        int port = 1521;
//        String isServiceName = databaseOracle.getIsServiceName() == 1 ? "/" : ":";
        String databaseName = "orcl";
        String url = String.format(rawUrl, hostname, port, databaseName);
        System.out.println(url);
    }

    @Test
    public void JavaTest() {
        double v = 777.777;
        System.out.println(v);

        byte[] bytes = SerializationUtil.doubleToBytes(v);

        double obj = SerializationUtil.bytesToDouble(bytes);

//        System.out.println(obj.getClass().toString());
        System.out.println(obj == v);
    }
}
