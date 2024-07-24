package com.atguigu.realtime.app;

import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class BaseSqlApp {
    public void initKafka(int port, int parallelism, String jobName) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 设置全局并行度为 2，表示在整个应用中，每个算子（operator）将有 2 个并行实例。
        env.setParallelism(parallelism);
        // 启用检查点机制，设置检查点的间隔为 3000 毫秒（3秒）。检查点用于确保数据处理的状态在发生故障时能够恢复。
        env.enableCheckpointing(3000);
        // 设置状态后端为 HashMapStateBackend，这是一种简单的状态后端，通常用于测试和本地开发。状态后端决定了 Flink 如何存储和管理作业的状态。
        env.setStateBackend(new HashMapStateBackend());
        // 设置检查点存储位置为 HDFS，具体路径为 hdfs://hadoop102:8020/gmall/DimApp。这意味着检查点数据将存储在 HDFS 上。
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/" + jobName);
        // 设置检查点模式为 EXACTLY_ONCE，确保每条记录在故障恢复时仅处理一次，提供强一致性保证。
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点超时时间为 20 秒。如果一个检查点在 20 秒内未完成，则会被丢弃。
        env.getCheckpointConfig().setCheckpointTimeout(20 * 1000);
        // 设置最大并发检查点数为 1。这意味着在上一个检查点完成之前，不会触发新的检查点。
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 设置两次检查点之间的最小间隔为 500 毫秒。这个参数防止检查点过于频繁地触发。
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 设置外部化检查点的清理策略为 RETAIN_ON_CANCELLATION，表示作业取消时保留检查点。这样可以在作业重新启动时恢复状态。
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        handle(env, tEnv);
    }

    protected abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv);

    // 读取ods_db数据
    public void readOdsDB(StreamTableEnvironment tEnv, String groupId) {
        tEnv
                .executeSql("CREATE TABLE ods_db( " +
                        "`database` STRING, " +
                        "`table` STRING, " +
                        "`type` STRING, " +
                        "`ts` BIGINT, " +
                        "`data` MAP<STRING,STRING>, " +
                        "`old` MAP<STRING,STRING>, " +
                        "pt AS proctime() " +       // lookup join需要
                        ")" + SqlUtil.getKafkaSource(Constant.TOPIC_ODS_DB, groupId));
    }

    // 读取base_dic
    public void readBaseDic(StreamTableEnvironment tEnv) {
        tEnv
                .executeSql("CREATE TABLE base_dic( " +
                        "dic_code STRING, " +
                        "dic_name STRING" +
                        ")" + SqlUtil.getMysqlSource("base_dic"));
    }
}
