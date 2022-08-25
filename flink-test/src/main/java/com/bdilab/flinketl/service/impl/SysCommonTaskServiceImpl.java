package com.bdilab.flinketl.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.bdilab.flinketl.entity.ComponentTableInput;
import com.bdilab.flinketl.entity.ComponentTableUpsert;
import com.bdilab.flinketl.entity.SysCommonTask;
import com.bdilab.flinketl.entity.UserTask;
import com.bdilab.flinketl.flink.FlinkParser;
import com.bdilab.flinketl.mapper.SysCommonTaskMapper;
import com.bdilab.flinketl.service.ComponentTableInputService;
import com.bdilab.flinketl.service.ComponentTableUpsertService;
import com.bdilab.flinketl.service.SysCommonTaskService;
import com.bdilab.flinketl.service.UserTaskService;
import com.bdilab.flinketl.utils.GlobalResultUtil;
import com.bdilab.flinketl.utils.ResultExecuter;
import com.bdilab.flinketl.utils.TaskType;
import com.bdilab.flinketl.utils.WholeVariable;
import com.bdilab.flinketl.utils.exception.InfoNotInDatabaseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author hcyong
 * @since 2021-07-03
 */
@Slf4j
@Service
public class SysCommonTaskServiceImpl extends ServiceImpl<SysCommonTaskMapper, SysCommonTask> implements SysCommonTaskService {

    @Value("${flink.maxParallelism}")
    int maxParallelism;
    @Value("${flink.isRemote}")
    boolean isRemote;
    @Value(("${yaml-path}"))
    String yamlPath;

    @Resource
    SysCommonTaskMapper commonTaskMapper;
    @Resource
    UserTaskService userTaskService;
    @Resource
    FlinkParser flinkParser;
    @Resource
    ComponentTableInputService tableInputService;
    @Resource
    ComponentTableUpsertService tableUpsertService;

    private static final List<JobClient> JOB_CLIENTS = new LinkedList<>();

    @Override
    public GlobalResultUtil<Integer> build(int taskType, String taskName, int parallelism, int userId) {
        log.info("执行创建任务，任务类型：" + taskType + " 任务名：" + taskName);
        TaskType type = TaskType.getInstance(taskType);
        if (type == null) {
            log.error("任务执行类型不存在");
            return GlobalResultUtil.errorMsg("任务类型错误");
        }
        return new ResultExecuter<Integer>() {
            @Override
            public Integer run() throws Exception {
                log.info("开始创建任务");
                SysCommonTask commonTask = SysCommonTask.builder()
                        .taskName(taskName)
                        .taskStatus(0)
                        .parallelism(parallelism)
                        .build();
                commonTaskMapper.insert(commonTask);
                int taskId = commonTask.getId();
                log.info(taskName + "任务id为：" + taskId);
                return taskId;
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<String> run(int taskId) {
        return new ResultExecuter<String>() {
            @Override
            public String run() throws Exception {
                System.out.println("-------------运行任务-------------");
                return runTask(taskId);
            }
        }.execute();
    }

    public String runTask(int taskId) throws Exception {
        SysCommonTask commonTask = commonTaskMapper.selectById(taskId);
        if (commonTask == null) {
            throw new InfoNotInDatabaseException("无法找到该信息");
        } else {
            if (commonTask.getTaskStatus()!=WholeVariable.NO_BEGIN && commonTask.getTaskStatus()!=WholeVariable.STOP) {
                return "running";
            }
        }

        int sourceInputId = commonTask.getFkInputId();
        int targetUpsertId = commonTask.getFkUpsertId();
        String sourceType = commonTask.getSourceDataType();
        String targetType = commonTask.getTargetDataType();

        int parallelism = commonTask.getParallelism();

        StreamExecutionEnvironment env = flinkParser.generateEnv(isRemote)
                .setParallelism(1);
        env.enableCheckpointing(4000L);

        //读取Source和Sink的列名和列类型
        ComponentTableInput tableInput=tableInputService.getById(sourceInputId);
        ComponentTableUpsert tableUpsert = tableUpsertService.getById(targetUpsertId);

        SingleOutputStreamOperator dataStreamSource = flinkParser.generateDataSource(env, sourceInputId, sourceType, yamlPath,tableInput);

        String[] steps = commonTask.getStepOrder().split(" ");

        // 解析ETL任务信息，配置Flink数据流算子
        for (String step : steps) {
            switch (step) {
                case WholeVariable.FIELD_FILTER:
                    dataStreamSource = flinkParser.filterColumn(dataStreamSource, commonTask.getFkFilterColumnId());
                    break;
                case WholeVariable.FIELD_SPLIT:
                    dataStreamSource = flinkParser.splitColumn(dataStreamSource, commonTask.getFkInputId(), commonTask.getFkSplitColumnId());
                    break;
                case WholeVariable.FIELD_EMPTY:
                    continue;
                default:
                    throw new Exception("error step");
            }
        }

        System.out.println(dataStreamSource.getTransformation());

        JobClient jobClient = flinkParser.executeJob(env, dataStreamSource, commonTask.getTaskName(), sourceInputId,targetUpsertId, targetType, taskId, yamlPath);

        if (jobClient != null) {
            JOB_CLIENTS.add(jobClient);
            commonTask.setJobId(jobClient.getJobID().toString());
//            commonTask.setTaskStatus(WholeVariable.RUNNING);
            commonTaskMapper.updateById(commonTask);
            return "success submit";
        } else {
            return "error submit";
        }
    }

    @Override
    public GlobalResultUtil<String> stop(int taskId) {
        return new ResultExecuter<String>() {
            @Override
            public String run() throws Exception {
                log.info("开始停止任务：" + taskId);
                SysCommonTask commonTask = commonTaskMapper.selectById(taskId);
                String jobId = commonTask.getJobId();
                boolean isCancelled = false;
                JobClient tempJobClient;
                Iterator<JobClient> jobClientIterator = JOB_CLIENTS.iterator();
                while (jobClientIterator.hasNext()) {
                    tempJobClient = jobClientIterator.next();
                    try {
                        if (tempJobClient.getJobID().toString().equals(jobId)) {
                            tempJobClient.cancel();
                            jobClientIterator.remove();
                            isCancelled = true;
                        }
                    } catch (IllegalStateException e) {
                        jobClientIterator.remove();
                        return "task is not running or is finished";
                    }
                }
                if (isCancelled) {
//                    commonTask.setTaskStatus(WholeVariable.STOP);
                    commonTaskMapper.updateById(commonTask);
                    return "success cancel";
                } else {
                    return "fail cancel / task is not running or is finished";
                }
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<Boolean> commitTask(int taskId, String stepOrder, int parallelism, int userId) {
        return new ResultExecuter<Boolean>() {
            @Override
            public Boolean run() throws Exception {
                log.info("用户：" + userId + " 提交了任务：" + taskId);
                SysCommonTask commonTask = commonTaskMapper.selectById(taskId);
                commonTask.setCommit(true);
                commonTask.setStepOrder(stepOrder);
                commonTask.setParallelism(parallelism);
                boolean commonTaskUpdateSuccess = commonTaskMapper.updateById(commonTask)==1;

                UserTask userTask = UserTask.builder()
                        .fkUserId(userId)
                        .fkSysCommonTaskId(taskId)
                        .build();
                boolean userTaskSaveSuccess = userTaskService.save(userTask);
                return commonTaskUpdateSuccess && userTaskSaveSuccess;
            }
        }.execute();
    }

    @Override
    public GlobalResultUtil<List<SysCommonTask>> getCommonTaskList(int userId) {
        return new ResultExecuter<List<SysCommonTask>>() {
            @Override
            public List<SysCommonTask> run() throws Exception {
                Iterator<JobClient> jobClientIterator = JOB_CLIENTS.iterator();
                JobClient tempJobClient;
                boolean isDone;
                while (jobClientIterator.hasNext()) {
                    tempJobClient = jobClientIterator.next();
                    String jobId = tempJobClient.getJobID().toString();
                    try {
                        isDone = tempJobClient.getJobStatus().isDone();
                        if (isDone) {
//                            commonTaskMapper.updateCommonTaskByJobId(WholeVariable.END, jobId);
                            jobClientIterator.remove();
                        }
                    } catch (IllegalStateException e) {
                        System.out.println("job: " + jobId + " has finished");
//                        commonTaskMapper.updateCommonTaskByJobId(WholeVariable.END, jobId);
                        jobClientIterator.remove();
                    }
                }
                List<UserTask> userTasks = userTaskService.getUserTaskByUserId(userId);
                List<SysCommonTask> taskList;
                List<Integer> idList = new LinkedList<>();
                for (UserTask userTask : userTasks) {
                    idList.add(userTask.getFkSysCommonTaskId());
                }
                taskList = commonTaskMapper.selectBatchIds(idList);
                return taskList;
            }
        }.execute();
    }


}
