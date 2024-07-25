package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 经过分析，订单明细表和取消订单明细表的数据来源、表结构都相同，差别只在业务过程和过滤条件，为了减少重复计算，将两张表公共的关联过程提取出来，形成订单预处理表。
 * 关联订单明细表、订单表、订单明细活动关联表、订单明细优惠券关联表四张事实业务表和字典表（维度业务表）形成订单预处理表，写入 Kafka 对应主题。
 * 本节形成的预处理表中要保留订单表的 type 和 old 字段，用于过滤订单明细数据和取消订单明细数据。
 */
public class DWD05DWDTradeOrderPreProcess extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD05DWDTradeOrderPreProcess().initKafka(2105, 2, "DWD05DWDTradeOrderPreProcess");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 设置ttl
        tEnv.getConfig().setIdleStateRetention(Duration.ofHours(1));
        // 1.读取ods_db
        readOdsDB(tEnv, "DWD05DWDTradeOrderPreProcess");
        // 2.读取字典表
        readBaseDic(tEnv);
        // 3.过滤order_detail
        Table orderDetail = tEnv
                .sqlQuery("SELECT " +
                        "`data`['id'] id, " +
                        "`data`['order_id'] order_id, " +
                        "`data`['sku_id'] sku_id, " +
                        "`data`['sku_name'] sku_name, " +
                        "CAST(CAST(`data`['order_price'] AS DECIMAL(16,2)) * CAST(`data`['sku_num'] AS INT) AS STRING) split_original_amount, " +
                        "`data`['sku_num'] sku_num, " +
                        "`data`['create_time'] create_time, " +
                        "`data`['source_type'] source_type, " +
                        "`data`['source_id'] source_id, " +
                        "`data`['split_total_amount'] split_total_amount, " +
                        "`data`['split_activity_amount'] split_activity_amount, " +
                        "`data`['split_coupon_amount'] split_coupon_amount, " +
                        "ts od_ts, " +
                        "pt " +
                        "FROM ods_db " +
                        "WHERE `database` = 'gmall' AND `table` = 'order_detail' AND `type` = 'insert' ");
        tEnv.createTemporaryView("od", orderDetail);
        // 4.过滤order_info
        Table order_info = tEnv
                .sqlQuery("SELECT " +
                        "`data`['id'] id, " +
                        "`data`['user_id'] user_id, " +
                        "`data`['province_id'] province_id, " +
                        "`data`['operate_time'] operate_time, " +
                        "`data`['order_status'] order_status, " +
                        "`type`, " +
                        "`old`, " +
                        "ts oi_ts, " +
                        "pt " +
                        "FROM ods_db " +
                        "WHERE `database` = 'gmall' AND `table` = 'order_info' AND `type` IN ('insert','update')");
        tEnv.createTemporaryView("oi", order_info);
        // 5.过滤order_detail_activity
        Table orderDetailActivity = tEnv
                .sqlQuery("SELECT " +
                        "`data`['order_detail_id'] order_detail_id, " +
                        "`data`['activity_id'] activity_id, " +
                        "`data`['activity_rule_id'] activity_rule_id " +
                        "FROM ods_db " +
                        "WHERE `database` = 'gmall' AND `table` = 'order_detail_activity' AND `type` = 'insert' ");
        tEnv.createTemporaryView("oda", orderDetailActivity);
        // 6.过滤order_detail_coupon
        Table orderDetailCoupon = tEnv
                .sqlQuery("SELECT " +
                        "`data`['order_detail_id'] order_detail_id, " +
                        "`data`['coupon_id'] coupon_id " +
                        "FROM ods_db " +
                        "WHERE `database` = 'gmall' AND `table` = 'order_detail_coupon' AND `type` = 'insert' ");
        tEnv.createTemporaryView("odc", orderDetailCoupon);
        // 7.5表join
        Table result = tEnv
                .sqlQuery("SELECT " +
                        "od.id, " +
                        "od.order_id, " +
                        "oi.user_id, " +
                        "oi.order_status, " +
                        "od.sku_id, " +
                        "od.sku_name, " +
                        "oi.province_id, " +
                        "oda.activity_id, " +
                        "oda.activity_rule_id, " +
                        "odc.coupon_id, " +
                        "DATE_FORMAT(od.create_time,'yyyy-MM-dd') date_id, " +
                        "od.create_time, " +
                        "date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id, " +
                        "oi.operate_time, " +
                        "od.source_id, " +
                        "od.source_type, " +
                        "dic.dic_name source_type_name, " +
                        "od.sku_num, " +
                        "od.split_original_amount, " +
                        "od.split_activity_amount, " +
                        "od.split_coupon_amount, " +
                        "od.split_total_amount, " +
                        "oi.`type`, " +
                        "oi.`old`, " +
                        "od.od_ts, " +
                        "oi.oi_ts, " +
                        "CURRENT_ROW_TIMESTAMP() row_op_ts " +      // 汇总数据时的系统时间
                        "FROM od " +
                        "   JOIN oi ON od.order_id = oi.id " +
                        "   JOIN base_dic FOR SYSTEM_TIME AS OF od.pt AS dic ON od.source_type = dic.dic_code " +
                        "   LEFT JOIN oda ON od.id = oda.order_detail_id " +
                        "   LEFT JOIN odc ON od.id = odc.order_detail_id");
        // 8.写入kafka
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
                        "row_op_ts TIMESTAMP_LTZ(3), " +
                        "PRIMARY KEY(id) NOT ENFORCED " +
                        ")" + SqlUtil.getUpsertKafkaSink(Constant.TOPIC_DWD_TRADE_ORDER_PRE_PROCESS));
        result.executeInsert("dwd_trade_order_pre_process");
    }

}
