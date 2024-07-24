package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DWD04DWDTradeCartAdd extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD04DWDTradeCartAdd().initKafka(2104, 2, "DWD04DWDTradeCartAdd");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1.读取ods_db的数据
        readOdsDB(tEnv, "DWD04DWDTradeCartAdd");
        // 2.过滤cart_info表的数据
        Table cartInfo = tEnv
                .sqlQuery(
                        "SELECT " +
                                "`data`['id'] id, " +
                                "`data`['user_id'] user_id, " +
                                "`data`['sku_id'] sku_id, " +
                                "`data`['source_id'] source_id, " +
                                "`data`['source_type'] source_type, " +
                                "IF(`type` = 'insert',CAST(`data`['sku_num'] AS INT),CAST(`data`['sku_num'] AS INT) - CAST(`old`['sku_num'] AS INT)) sku_num, " +
                                "ts, " +
                                "pt " +
                                "FROM ods_db " +
                                "WHERE `type` = 'insert' OR ( `type` = 'update' AND `old`['sku_num'] IS NOT NULL AND CAST(`data`['sku_num'] AS INT) > CAST(`old`['sku_num'] AS INT)) " +
                                "AND `database` = 'gmall' AND `table` = 'cart_info'"
                );
        tEnv.createTemporaryView("cart_info", cartInfo);
        // 3.读取字典表
        readBaseDic(tEnv);
        // 4.维度退化
        Table result = tEnv
                .sqlQuery("SELECT " +
                        "cart_info.id, " +
                        "cart_info.user_id, " +
                        "cart_info.sku_id, " +
                        "cart_info.source_id, " +
                        "cart_info.source_type, " +
                        "d.dic_name source_type_name, " +
                        "cart_info.sku_num, " +
                        "cart_info.ts " +
                        "FROM cart_info " +
                        "   JOIN base_dic FOR SYSTEM_TIME AS OF cart_info.pt AS d ON cart_info.source_type = d.dic_code");
        // 5.写入kafka
        tEnv
                .executeSql("CREATE TABLE dwd_trade_cart_add( " +
                        "id STRING, " +
                        "user_id STRING, " +
                        "sku_id STRING, " +
                        "source_id STRING, " +
                        "source_type_code STRING, " +
                        "source_type_name STRING, " +
                        "sku_num INT, " +
                        "ts BIGINT " +
                        ")" + SqlUtil.getKafkaSink(Constant.TOPIC_DWD_TRADE_CART_ADD));
        result.executeInsert("dwd_trade_cart_add");
    }
}
