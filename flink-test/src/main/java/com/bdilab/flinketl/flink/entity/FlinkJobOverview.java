package com.bdilab.flinketl.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * @author hcyong
 * @date 2021/7/29
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FlinkJobOverview implements Serializable {
    String jid;
    String name;
    String state;
    long startTime;
    long endTime;
    long duration;
    long lastModification;
    Map<String, Integer> tasks;
}
