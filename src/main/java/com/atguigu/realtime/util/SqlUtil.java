package com.atguigu.realtime.util;

import com.atguigu.realtime.common.Constant;

public class SqlUtil {
    public static String getKafkaSource(String topic, String groupId) {
        return " WITH ( " +
                "'connector' = 'kafka', " +
                "'topic' = '" + topic + "', " +
                "'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
                "'properties.group.id' = '" + groupId + "', " +
                "'scan.startup.mode' = 'latest-offset', " +
                "'format' = 'json' " +
                ")";
    }

    public static String getMysqlSource(String tableName) {
        return " WITH ( " +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://" + Constant.MYSQL_HOSTNAME_GMALL + ":" + Constant.MYSQL_PORT_GMALL + "/gmall?useSSL=false', " +
                "'table-name' = '" + tableName + "', " +
                "'username' = '" + Constant.MYSQL_USERNAME_GMALL + "', " +
                "'password' = '" + Constant.MYSQL_PASSWORD_GMALL + "', " +
                "'lookup.cache.max-rows' = '10', " +
                "'lookup.cache.ttl' = '1 hour' " +
                ")";
    }

    public static String getKafkaSink(String topic) {
        return "WITH( " +
                "'connector' = 'kafka', " +
                "'topic' = '" + topic + "', " +
                "'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
                " 'format'='json' " +
                ")";
    }
}
