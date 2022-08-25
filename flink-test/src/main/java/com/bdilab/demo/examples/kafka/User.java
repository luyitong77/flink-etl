package com.bdilab.demo.examples.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @description:
 * @author: ljw
 * @time: 2021/9/16 19:42
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class User {
    String id;
    String name;
    String age;
}
