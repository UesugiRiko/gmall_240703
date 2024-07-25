package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 交易域下单事务事实表
 */
public class DWD06DWDTradeOrderDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD06DWDTradeOrderDetail().initKafka(2106, 2, "DWD06DWDTradeOrderDetail");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1.读预处理表
        tEnv
                .executeSql("CREATE TABLE dwd_trade_order_pre_process( " +
                        "id STRING, " +
                        "order_id STRING, " +
                        "user_id STRING, " +
                        "order_status STRING, " +
                        "sku_id STRING, " +
                        "sku_name STRING, " +
                        "province_id STRING, " +
                        "activity_id STRING, " +
                        "activity_rule_id STRING, " +
                        "coupon_id STRING, " +
                        "date_id STRING, " +
                        "create_time STRING, " +
                        "operate_date_id STRING, " +
                        "operate_time STRING, " +
                        "source_id STRING, " +
                        "source_type STRING, " +
                        "source_type_name STRING, " +
                        "sku_num STRING, " +
                        "split_original_amount STRING, " +
                        "split_activity_amount STRING, " +
                        "split_coupon_amount STRING, " +
                        "split_total_amount STRING, " +
                        "`type` STRING, " +
                        "`old` MAP<STRING,STRING>, " +
                        "od_ts BIGINT, " +
                        "oi_ts BIGINT, " +
                        "row_op_ts TIMESTAMP_LTZ(3) " +
                        ")" + SqlUtil.getKafkaSource(Constant.TOPIC_DWD_TRADE_ORDER_PRE_PROCESS, "DWD06DWDTradeOrderDetail"));
        // 2.筛选下单详情
        Table result = tEnv
                .sqlQuery("SELECT " +
                        "id, " +
                        "order_id, " +
                        "user_id, " +
                        "sku_id, " +
                        "sku_name, " +
                        "province_id, " +
                        "activity_id, " +
                        "activity_rule_id, " +
                        "coupon_id, " +
                        "date_id, " +
                        "create_time, " +
                        "source_id, " +
                        "source_type, " +
                        "source_type_name, " +
                        "sku_num, " +
                        "split_original_amount, " +
                        "split_activity_amount, " +
                        "split_coupon_amount, " +
                        "split_total_amount, " +
                        "od_ts, " +
                        "row_op_ts " +
                        "FROM dwd_trade_order_pre_process " +
                        "WHERE `type` = 'insert'");
        // 3.写入Kafka
        tEnv
                .executeSql("CREATE TABLE dwd_trade_order_detail( " +
                        "id STRING, " +
                        "order_id STRING, " +
                        "user_id STRING, " +
                        "sku_id STRING, " +
                        "sku_name STRING, " +
                        "province_id STRING, " +
                        "activity_id STRING, " +
                        "activity_rule_id STRING, " +
                        "coupon_id STRING, " +
                        "date_id STRING, " +
                        "create_time STRING, " +
                        "source_id STRING, " +
                        "source_type_code STRING, " +
                        "source_type_name STRING, " +
                        "sku_num STRING, " +
                        "split_original_amount STRING, " +
                        "split_activity_amount STRING, " +
                        "split_coupon_amount STRING, " +
                        "split_total_amount STRING, " +
                        "ts BIGINT, " +
                        "row_op_ts TIMESTAMP_LTZ(3) " +
                        ")" + SqlUtil.getKafkaSink(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
        result.executeInsert("dwd_trade_order_detail");
    }
}
