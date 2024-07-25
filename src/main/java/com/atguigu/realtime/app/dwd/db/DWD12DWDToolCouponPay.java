package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 工具域优惠券使用(支付)事务事实表
 */
public class DWD12DWDToolCouponPay extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD12DWDToolCouponPay().initKafka(2112, 2, "DWD12DWDToolCouponPay");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1.ods_db中获取领券表，筛选用券数据
        readOdsDB(tEnv, "DWD12DWDToolCouponPay");
        Table result = tEnv
                .sqlQuery("SELECT " +
                        "`data`['id'] id, " +
                        "`data`['coupon_id'] coupon_id, " +
                        "`data`['order_id'] order_id, " +
                        "`data`['user_id'] user_id, " +
                        "DATE_FORMAT(`data`['used_time'],'yyyy-MM-dd'), " +
                        "`data`['used_time'], " +
                        "ts " +
                        "FROM ods_db " +
                        "WHERE `database` =  'gmall' AND `table` = 'coupon_use' AND `type` = 'update' AND `old`['used_time'] IS NULL AND `data`['used_time'] IS NOT NULL");
        // 2.写入kafka
        tEnv
                .executeSql("CREATE TABLE dwd_tool_coupon_pay( " +
                        "id STRING, " +
                        "coupon_id STRING, " +
                        "order_id STRING, " +
                        "user_id STRING, " +
                        "date_id STRING, " +
                        "payment_time STRING, " +
                        "ts BIGINT " +
                        ")" + SqlUtil.getKafkaSink(Constant.TOPIC_DWD_TOOL_COUPON_PAY));
        result.executeInsert("dwd_tool_coupon_pay");
    }
}
