package com.atguigu.realtime.util;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;


public class FlinkSourceUtil {
    /**
     * 自定义KafkaSource
     *
     * @param groupId
     * @param topic
     * @return KafkaSource<String>
     */
    public static KafkaSource<String> getKafkaSource(String groupId, String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("isolation.level", "read_committed ")
                .build();
    }

    /**
     * 自定义MysqlSource,flink cdc
     *
     * @param databaseList 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*"
     * @param tableList    设置捕获的表,"yourDatabaseName.yourTableName"
     * @return MySqlSource<String>
     */
    public static MySqlSource<String> getMysqlSource(String databaseList, String tableList) {
        return MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOSTNAME_GMALL)
                .port(Constant.MYSQL_PORT_GMALL)
                .databaseList(databaseList) // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList(tableList) // 设置捕获的表
                .username(Constant.MYSQL_USERNAME_GMALL)
                .password(Constant.MYSQL_PASSWORD_GMALL)
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
    }
}
