package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 互动域评价事务事实表
 */
public class DWD15DWDInteractionComment extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD15DWDInteractionComment().initKafka(2115, 2, "DWD15DWDInteractionComment");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1.设置TTL
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        // 2.读取ods_db中comment_info数据
        readOdsDB(tEnv, "DWD15DWDInteractionComment");
        Table ci = tEnv
                .sqlQuery("SELECT " +
                        "`data`['id'] id, " +
                        "`data`['user_id'] user_id, " +
                        "`data`['sku_id'] sku_id, " +
                        "`data`['order_id'] order_id, " +
                        "`data`['create_time'] create_time, " +
                        "`data`['appraise'] appraise, " +
                        "ts, " +
                        "pt " +
                        "FROM ods_db " +
                        "WHERE `database` =  'gmall' AND `table` = 'comment_info' AND `type` = 'insert'");
        tEnv.createTemporaryView("ci", ci);
        // 3.读取base_dic表
        readBaseDic(tEnv);
        // 4.2表join
        Table result = tEnv
                .sqlQuery("SELECT " +
                        "ci.id, " +
                        "ci.user_id, " +
                        "ci.sku_id , " +
                        "ci.order_id, " +
                        "DATE_FORMAT(ci.create_time,'yyyy-MM-dd'), " +
                        "ci.create_time, " +
                        "ci.appraise, " +
                        "dic.dic_name, " +
                        "ts " +
                        "FROM ci " +
                        "JOIN base_dic FOR SYSTEM_TIME AS OF ci.pt AS dic ON ci.appraise = dic.dic_code");
        // 5.写入kafka
        tEnv
                .executeSql("CREATE TABLE dwd_interaction_comment( " +
                        "id STRING," +
                        "user_id STRING," +
                        "sku_id STRING," +
                        "order_id STRING," +
                        "data_id STRING," +
                        "create_time STRING," +
                        "appraise_code STRING," +
                        "appraise_name STRING," +
                        "ts BIGINT " +
                        ")" + SqlUtil.getKafkaSink(Constant.TOPIC_DWD_INTERACTION_COMMENT));
        result.executeInsert("dwd_interaction_comment");
    }
}
