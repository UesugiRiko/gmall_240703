package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 工具域优惠券使用(下单)事务事实表
 */
public class DWD13DWDToolCouponOrder extends BaseSqlApp {
    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1.ods_db获取领券表，筛选下单数据
        readOdsDB(tEnv, "DWD13DWDToolCouponOrder");
        Table result = tEnv
                .sqlQuery("SELECT " +
                        "`data`['id'] id, " +
                        "`data`['coupon_id'] coupon_id, " +
                        "`data`['order_id'] order_id, " +
                        "`data`['user_id'] user_id, " +
                        "DATE_FORMAT(`data`['using_time'],'yyyy-MM-dd'), " +
                        "`data`['using_time'], " +
                        "ts " +
                        "FROM ods_db " +
                        "WHERE `database` =  'gmall' AND `table` = 'coupon_use' AND `type` = 'update' AND `old`['coupon_status'] = '1401' AND `data`['coupon_status'] = '1402' ");
        // 2.写入kafka
        tEnv
                .executeSql("CREATE TABLE dwd_tool_coupon_order( " +
                        "id STRING, " +
                        "coupon_id STRING, " +
                        "order_id STRING, " +
                        "user_id STRING, " +
                        "date_id STRING, " +
                        "order_time STRING, " +
                        "ts BIGINT " +
                        ")" + SqlUtil.getKafkaSink(Constant.TOPIC_DWD_TOOL_COUPON_ORDER));
        result.executeInsert("dwd_tool_coupon_order");
    }

    public static void main(String[] args) {
        new DWD13DWDToolCouponOrder().initKafka(2113, 2, "DWD13DWDToolCouponOrder");
    }
}
