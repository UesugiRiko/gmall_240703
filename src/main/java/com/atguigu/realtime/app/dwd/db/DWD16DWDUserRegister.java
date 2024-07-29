package com.atguigu.realtime.app.dwd.db;

import com.atguigu.realtime.app.BaseSqlApp;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 用户域用户注册事务事实表
 */
public class DWD16DWDUserRegister extends BaseSqlApp {
    public static void main(String[] args) {
        new DWD16DWDUserRegister().initKafka(2116, 2, "DWD16DWDUserRegister");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1.读取ods_db中的user_info表
        readOdsDB(tEnv, "DWD16DWDUserRegister");
        Table result = tEnv
                .sqlQuery("SELECT " +
                        "`data`['id'], " +
                        "DATE_FORMAT(`data`['create_time'],'yyyy-MM-dd'), " +
                        "`data`['create_time'], " +
                        "ts " +
                        "FROM ods_db " +
                        "WHERE `database` =  'gmall' AND `table` = 'user_info' AND `type` = 'insert'");
        // 2.写入kafka
        tEnv
                .executeSql("CREATE TABLE dwd_user_register( " +
                        "user_id STRING, " +
                        "date_id STRING, " +
                        "create_time STRING, " +
                        "ts BIGINT " +
                        ")" + SqlUtil.getKafkaSink(Constant.TOPIC_DWD_USER_REGISTER));
        result.executeInsert("dwd_user_register");
    }
}
