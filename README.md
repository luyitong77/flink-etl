# 1.ETL任务的运行流程
## 1.通过DatabaseXXXController创建数据源
 - 1.配置数据源相关信息
 - 2.测试连接是否成功
 - 3.保存数据源配置信息
## 2.通过SysCommonTaskController创建ETL任务
 - 1.使用build接口新建任务
 - 2.选择源数据源
 - 3.通过ComponentXXXController创建转换组件
 - 4.选择目标数据源
 - 5.使用commitTask接口保存ETL任务
 - 6.使用run接口执行ETL任务

# 2.配置Flink数据流的流程（runTask接口）
 - 1.获取源与目标数据源的类型及配置信息
 - 2.获取Flink数据流环境
 - 3.获取转换组件的顺序
 - 4.根据转换组件信息配置Flink数据流
 - 5.提交Flink任务到远程集群中运行

# 3.注意要点
 - 1.application.yaml需要保存在每个Flink集群所在服务器中，因为Flink集群节点只会读取本机中的文件，同时需要修改对应的路径
 - 2.application.yaml文件中Flink的ip与port对应访问Flink webUI时的地址
 - 3.Flink集群需要hdfs环境保存checkpoint
 - 4.csv使用hdfs环境保存文件（因为Flink是集群，直接保存会分散在各个节点中）
 - 5.数据源对应的版本可以参考**src/main/resources/performance-test.xlsx**
