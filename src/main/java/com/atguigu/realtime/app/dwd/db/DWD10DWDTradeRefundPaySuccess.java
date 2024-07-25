package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 交易域退款成功事务事实表
 */
// TODO:2024/07/25
public class DWD10DWDTradeRefundPaySuccess extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD10DWDTradeRefundPaySuccess().initKafka(2110, 2, "DWD10DWDTradeRefundPaySuccess");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        readOdsDB(tEnv, "DWD10DWDTradeRefundPaySuccess");
        // 1.ods_db中获取退款表
        tEnv
                .sqlQuery("SELECT " +
                        "FROM ");
        // 2.ods_db中获取退单表
        // 3.ods_db中订单表过滤数据
        // 4.读取字典表
        // 5.写入kafka
    }
}
