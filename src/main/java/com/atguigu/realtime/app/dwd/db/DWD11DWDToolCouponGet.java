package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 工具域优惠券领取事务事实表
 */
public class DWD11DWDToolCouponGet extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD11DWDToolCouponGet().initKafka(2111, 2, "DWD11DWDToolCouponGet");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1.ods_db获取领券表
        readOdsDB(tEnv, "DWD11DWDToolCouponGet");
        Table result = tEnv
                .sqlQuery("SELECT " +
                        "`data`['id'] id, " +
                        "`data`['coupon_id'] coupon_id, " +
                        "`data`['user_id'] user_id, " +
                        "DATE_FORMAT(`data`['get_time'],'yyyy-MM-dd'), " +
                        "`data`['get_time'], " +
                        "ts " +
                        "FROM ods_db " +
                        "WHERE `database` = 'gmall' AND `table` = 'coupon_use' AND `type` = 'insert' ");
        // 2.写入kafka
        tEnv
                .executeSql("CREATE TABLE dwd_tool_coupon_get( " +
                        "id STRING, " +
                        "coupon_id STRING, " +
                        "user_id STRING, " +
                        "date_id STRING, " +
                        "get_time STRING, " +
                        "ts BIGINT " +
                        ")" + SqlUtil.getKafkaSink(Constant.TOPIC_DWD_TOOL_COUPON_GET));
        result.executeInsert("dwd_tool_coupon_get");
    }
}
