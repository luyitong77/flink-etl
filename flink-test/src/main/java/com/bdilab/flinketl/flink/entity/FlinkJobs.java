package com.bdilab.flinketl.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author hcyong
 * @date 2021/7/29
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FlinkJobs {
    FlinkJobOverview[] jobs;
}
