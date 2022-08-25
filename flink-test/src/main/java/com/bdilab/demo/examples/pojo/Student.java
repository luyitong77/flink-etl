package com.bdilab.demo.examples.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @author hcyong
 * @date 2021/7/19
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Student {
    private int id;
    private String name;
    private String password;
    private int age;
    private String father;
    private String mother;
    private String address;
    private String province;
    private Date birthday;
    private int grade;
    private int chinese;
    private int math;
    private int english;
    private int physics;
    private int chemistry;
    private int biology;
    private int politics;
    private int history;
    private int geography;
    private int pe;
    private int art;
    private int music;
}

