package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 交易域退单事务事实表
 */
public class DWD09DWDTradeOrderRefund extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD09DWDTradeOrderRefund().initKafka(2109, 2, "DWD09DWDTradeOrderRefund");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1.设置TTL
        tEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(10));
        readOdsDB(tEnv, "DWD09DWDTradeOrderRefund");
        // 2.ods_db筛选退单表
        Table orderRefundInfo = tEnv
                .sqlQuery("SELECT " +
                        "`data`['id'] id, " +
                        "`data`['user_id'] user_id, " +
                        "`data`['order_id'] order_id, " +
                        "`data`['sku_id'] sku_id, " +
                        "`data`['refund_type'] refund_type, " +
                        "`data`['refund_num'] refund_num, " +
                        "`data`['refund_amount'] refund_amount, " +
                        "`data`['refund_reason_type'] refund_reason_type, " +
                        "`data`['refund_reason_txt'] refund_reason_txt, " +
                        "`data`['create_time'] create_time, " +
                        "pt, " +
                        "ts " +
                        "FROM ods_db " +
                        "WHERE `database` = 'gmall' AND `table` = 'order_refund_info' AND `type` = 'insert' ");
        tEnv.createTemporaryView("ri", orderRefundInfo);
        // 3.ods_db过滤订单表，获取退单数据
        Table orderInfo = tEnv
                .sqlQuery("SELECT " +
                        "`data`['id'] id, " +
                        "`data`['province_id'] province_id, " +
                        "`old` " +
                        "FROM ods_db " +
                        "WHERE `database` = 'gmall' AND `table` = 'order_info' AND `type` = 'update' AND `old`['order_status'] IS NOT NULL AND `data`['order_status'] = '1005'");
        tEnv.createTemporaryView("oi", orderInfo);
        // 4.读取字典表
        readBaseDic(tEnv);
        // 5.3表join
        Table result = tEnv
                .sqlQuery("SELECT " +
                        "ri.id, " +
                        "ri.user_id, " +
                        "ri.order_id, " +
                        "ri.sku_id, " +
                        "oi.province_id, " +
                        "DATE_FORMAT(ri.create_time,'yyyy-MM-dd'), " +
                        "ri.create_time, " +
                        "ri.refund_type, " +
                        "dic1.dic_name, " +
                        "ri.refund_reason_type, " +
                        "dic2.dic_name, " +
                        "ri.refund_reason_txt, " +
                        "ri.refund_num, " +
                        "ri.refund_amount, " +
                        "ri.ts, " +
                        "CURRENT_ROW_TIMESTAMP() row_op_ts " +
                        "FROM ri " +
                        "   JOIN oi ON ri.order_id = oi.id " +
                        "   JOIN base_dic FOR SYSTEM_TIME AS OF ri.pt AS dic1 ON dic1.dic_code = ri.refund_type " +
                        "   JOIN base_dic FOR SYSTEM_TIME AS OF ri.pt AS dic2 ON dic2.dic_code = ri.refund_reason_type ");
        // 6.写入Kafka
        tEnv
                .executeSql("CREATE TABLE dwd_trade_order_refund( " +
                        "id STRING, " +
                        "user_id STRING, " +
                        "order_id STRING, " +
                        "sku_id STRING, " +
                        "province_id STRING, " +
                        "date_id STRING, " +
                        "create_time STRING, " +
                        "refund_type_code STRING, " +
                        "refund_type_name STRING, " +
                        "refund_reason_type_code STRING, " +
                        "refund_reason_type_name STRING, " +
                        "refund_reason_txt STRING, " +
                        "refund_num STRING, " +
                        "refund_amount STRING, " +
                        "ts BIGINT, " +
                        "row_op_ts TIMESTAMP_LTZ(3) " +
                        ")" + SqlUtil.getKafkaSink(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));
        result.executeInsert("dwd_trade_order_refund");
    }
}
