package com.bdilab.demo.examples.test;

import com.bdilab.demo.examples.pojo.Student;
import com.bdilab.demo.examples.service.StudentService;
import com.bdilab.demo.examples.service.TestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author hcyong
 * @date 2021/7/12
 */
@RestController
@RequestMapping("/test")
public class TestController {
    @Autowired
    TestService testService;
    @Autowired
    StudentService studentService;

    @GetMapping("/flink1")
    public ResponseEntity testFlink1() throws Exception {
        return ResponseEntity.ok(testService.testFlink());
//        return "success";
    }

    @GetMapping("/flink2")
    public ResponseEntity testFlink2() throws Exception {
        return ResponseEntity.ok(testService.testFlink1());
//        return "success";
    }

    @GetMapping("/batchInsert")
    public boolean batchInsert() throws Exception {
//        int num = 1000000;
//        List<Student> list = new ArrayList<>();
//        Random random = new Random();
//        for (int i=0; i<num; ++i) {
//            System.out.println(i);
//            list.add(Student.builder()
//                    .name(generateRandomString())
//                    .password(generateRandomString())
//                    .age(random.nextInt(88))
//                    .build());
//        }
//        return studentService.saveBatch(list);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String[] provinces = {
                "HeBei", "ShanXi", "LiaoNing", "JiLin", "HeiLongJiang", "JiangSu", "ZheJiang",
                "AnHui", "FuJian", "JiangXi", "ShanDong", "HeNan", "HuBei", "HuNan",
                "GuangDong", "HaiNan", "SiChuan", "GuiZhou", "YunNan", "ShaanXi", "GanSu",
                "QingHai", "TaiWan", "NeiMengGu", "GuangXi", "XiZang", "NingXia", "XinJiang",
                "BeiJing", "TianJin", "ShangHai", "ChongQing", "XiangGang", "AoMen"
        };
        int num = 10000;
        int count = 0;
        List<Student> list;
        Random random = new Random();
        for (int j = 0; j < 100; ++j) {
            list = new ArrayList<>();
            for (int i = 0; i < num / 100; ++i) {
                list.add(Student.builder()
                        .name(generateRandomString())
                        .password(generateRandomString())
                        .age(random.nextInt(88))
                        .father(generateRandomString())
                        .mother(generateRandomString())
                        .address(generateRandomString())
                        .province(provinces[random.nextInt(34)])
                        .birthday(simpleDateFormat.parse(generateandomDate()))
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
            if (studentService.saveBatch(list)) {
                ++count;
            }
            System.out.println(count);
        }

        return count == (num / 100);
    }

    String generateRandomString() {
        String str = "aAbBcCdDeEfFgGhHiIjJkKlLmMnNoOpPqQrRsStTuUvVwWxXyYzZ0123456789";
        Random random = new Random();
        StringBuffer stringBuffer = new StringBuffer();
        //确定字符串长度
        int stringLength = (int) (Math.random() * 10);
        for (int j = 0; j < stringLength; j++) {
            int index = random.nextInt(str.length());
            char c = str.charAt(index);
            stringBuffer.append(c);
        }
        //将StringBuffer转换为String类型的字符串
        return stringBuffer.toString();
    }


    private String generateandomDate() {
        Random rndYear = new Random();
        int year = rndYear.nextInt(18) + 2000;
        Random rndMonth = new Random();
        int month = rndMonth.nextInt(12) + 1;
        Random rndDay = new Random();
        int Day = rndDay.nextInt(28) + 1;
        Random rndHour = new Random();
        int hour = rndHour.nextInt(23);
        Random rndMinute = new Random();
        int minute = rndMinute.nextInt(60);
        Random rndSecond = new Random();
        int second = rndSecond.nextInt(60);
        return year + "-" + cp(month) + "-" + cp(Day) + "  " + cp(hour) + ":" + cp(minute) + ":" + cp(second);
    }

    private String cp(int num) {
        String Num = num + "";
        if (Num.length() == 1) {
            return "0" + Num;
        } else {
            return Num;
        }
    }

}
