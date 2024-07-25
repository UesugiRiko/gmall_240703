package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 互动域收藏商品事务事实表
 */
public class DWD14DWDInteractionFavorAdd extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD14DWDInteractionFavorAdd().initKafka(2114, 2, "DWD14DWDInteractionFavorAdd");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1.读取ods_db中的favor_info表的数据
        readOdsDB(tEnv, "DWD14DWDInteractionFavorAdd");
        tEnv
                .sqlQuery("SELECT " +
                        "`data`['id'] id, " +
                        "`data`['user_id'] user_id, " +
                        "`data`['sku_id'] sku_id, " +
                        "DATE_FORMAT(`data`['create_time'],'yyyy-MM-dd'), " +
                        "`data`['create_time'], " +
                        "ts " +
                        "FROM ods_db " +
                        "WHERE `database` = 'gmall' AND `table` = 'favor_info' AND `type` = 'insert' ");
        // 2.写入kafka
        tEnv
                .executeSql("CREATE TABLE dwd_interaction_favor_add( " +
                        "id STRING, " +
                        "user_id STRING, " +
                        "sku_id STRING, " +
                        "date_id STRING, " +
                        "order_time STRING, " +
                        "ts BIGINT " +
                        ")" + SqlUtil.getKafkaSink(Constant.TOPIC_DWD_INTERACTION_FAVOR_ADD));
    }
}
