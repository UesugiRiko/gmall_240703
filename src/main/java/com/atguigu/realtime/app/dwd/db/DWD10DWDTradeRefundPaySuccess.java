package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

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
        // 1.设置TTL
        tEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(10));
        // 2.ods_db中获取退单表
        Table ori = tEnv
                .sqlQuery("SELECT " +
                        "`data`['order_id'] order_id, " +
                        "`data`['sku_id'] sku_id, " +
                        "`data`['refund_num'] refund_num " +
                        "FROM ods_db " +
                        "WHERE `database` =  'gmall' AND `table` = 'order_refund_info' AND `type` = 'insert'");
        tEnv.createTemporaryView("ori", ori);
        // 3.ods_db中获取退款表
        Table rp = tEnv
                .sqlQuery("SELECT " +
                        "data['id'] id, " +
                        "data['order_id'] order_id, " +
                        "data['sku_id'] sku_id, " +
                        "data['payment_type'] payment_type, " +
                        "data['callback_time'] callback_time, " +
                        "data['total_amount'] total_amount, " +
                        "pt, " +
                        "ts " +
                        "FROM ods_db " +
                        "WHERE `database` =  'gmall' AND `table` = 'refund_payment' ");
        tEnv.createTemporaryView("rp", rp);
        // 4.ods_db中订单表过滤数据
        Table oi = tEnv
                .sqlQuery("SELECT " +
                        "data['id'] id, " +
                        "data['user_id'] user_id, " +
                        "data['province_id'] province_id, " +
                        "`old` " +
                        "FROM ods_db " +
                        "WHERE `database` =  'gmall' AND `table` = 'order_info' AND `type` = 'update' ");
        tEnv.createTemporaryView("oi", oi);
        // 5.读取字典表
        readBaseDic(tEnv);
        // 6.4表join
        Table result = tEnv
                .sqlQuery("SELECT " +
                        "rp.id, " +
                        "oi.user_id, " +
                        "rp.order_id, " +
                        "rp.sku_id, " +
                        "oi.province_id, " +
                        "rp.payment_type, " +
                        "dic.dic_name payment_type_name, " +
                        "date_format(rp.callback_time,'yyyy-MM-dd') date_id, " +
                        "rp.callback_time, " +
                        "ori.refund_num, " +
                        "rp.total_amount, " +
                        "rp.ts, " +
                        "current_row_timestamp() row_op_ts " +
                        "FROM rp " +
                        "JOIN ori ON rp.order_id = ori.order_id AND rp.sku_id = ori.sku_id " +
                        "JOIN oi ON rp.order_id = oi.id " +
                        "JOIN base_dic FOR SYSTEM_TIME AS OF rp.pt AS dic ON rp.payment_type = dic.dic_code ");
        // 7.写入kafka
        tEnv.executeSql("create table dwd_trade_refund_pay_suc( " +
                "id string, " +
                "user_id string, " +
                "order_id string, " +
                "sku_id string, " +
                "province_id string, " +
                "payment_type_code string, " +
                "payment_type_name string, " +
                "date_id string, " +
                "callback_time string, " +
                "refund_num string, " +
                "refund_amount string, " +
                "ts bigint, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + SqlUtil.getKafkaSink(Constant.TOPIC_DWD_TRADE_REFUND_PAY_SUC));
        result.executeInsert("dwd_trade_refund_pay_suc");
    }
}
