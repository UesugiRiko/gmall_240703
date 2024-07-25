package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 交易域支付成功事务事实表
 */
public class DWD08DWDTradePaySuccessDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD08DWDTradePaySuccessDetail().initKafka(2108, 2, "DWD08DWDTradePaySuccessDetail");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1.读取下单表
        tEnv
                .executeSql("CREATE TABLE od( " +
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
                        ")" + SqlUtil.getKafkaSource(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, "DWD08DWDTradePaySuccessDetail"));
        // 2.ods_db筛选支付成功信息
        readOdsDB(tEnv, "DWD08DWDTradePaySuccessDetail");
        Table paymentInfo = tEnv
                .sqlQuery("SELECT " +
                        "`data`['user_id'] user_id, " +
                        "`data`['order_id'] order_id, " +
                        "`data`['payment_type'] payment_type, " +
                        "data['callback_time'] callback_time, " +
                        "pt, " +
                        "ts " +
                        "FROM ods_db " +
                        "WHERE `database` = 'gmall' AND `table` = 'payment_info' AND `type` = 'update' AND `old`['payment_status'] IS NOT NULL AND `data`['payment_status'] = '1602'");
        tEnv.createTemporaryView("pi", paymentInfo);
        // 3.读取字典表
        readBaseDic(tEnv);
        // 4.3表join
        Table result = tEnv
                .sqlQuery("SELECT " +
                        "od.id, " +
                        "od.order_id, " +
                        "od.user_id, " +
                        "od.sku_id, " +
                        "od.sku_name, " +
                        "od.province_id, " +
                        "od.activity_id, " +
                        "od.activity_rule_id, " +
                        "od.coupon_id, " +
                        "pi.payment_type, " +
                        "dic.dic_name, " +
                        "pi.callback_time, " +
                        "od.source_id, " +
                        "od.source_type_code, " +
                        "od.source_type_name, " +
                        "od.sku_num, " +
                        "od.split_original_amount, " +
                        "od.split_activity_amount, " +
                        "od.split_coupon_amount, " +
                        "od.split_total_amount, " +
                        "pi.ts, " +
                        "od.row_op_ts " +
                        "FROM pi " +
                        "   JOIN od ON pi.order_id = od.order_id " +
                        "   JOIN base_dic FOR SYSTEM_TIME AS OF pi.pt AS dic ON pi.payment_type = dic.dic_code ");
        // 5.写入Kafka
        tEnv
                .executeSql("CREATE TABLE dwd_trade_pay_detail_suc( " +
                        "order_detail_id STRING, " +
                        "order_id STRING, " +
                        "user_id STRING, " +
                        "sku_id STRING, " +
                        "sku_name STRING, " +
                        "province_id STRING, " +
                        "activity_id STRING, " +
                        "activity_rule_id STRING, " +
                        "coupon_id STRING, " +
                        "payment_type_code STRING, " +
                        "payment_type_name STRING, " +
                        "callback_time STRING, " +
                        "source_id STRING, " +
                        "source_type_code STRING, " +
                        "source_type_name STRING, " +
                        "sku_num STRING ," +
                        "split_original_amount STRING, " +
                        "split_activity_amount STRING, " +
                        "split_coupon_amount STRING, " +
                        "split_total_amount STRING, " +
                        "ts BIGINT, " +
                        "row_op_ts TIMESTAMP_LTZ(3) " +
                        ")" + SqlUtil.getKafkaSink(Constant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC));
        result.executeInsert("dwd_trade_pay_detail_suc");
    }
}
