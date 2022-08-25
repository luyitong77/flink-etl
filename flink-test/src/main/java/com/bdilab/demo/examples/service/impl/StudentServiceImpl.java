package com.bdilab.demo.examples.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bdilab.demo.examples.mapper.StudentMapper;
import com.bdilab.demo.examples.pojo.Student;
import com.bdilab.demo.examples.service.StudentService;
import org.springframework.stereotype.Service;

/**
 * @author hcyong
 * @date 2021/7/19
 */
@Service
public class StudentServiceImpl extends ServiceImpl<StudentMapper, Student> implements StudentService {
}
