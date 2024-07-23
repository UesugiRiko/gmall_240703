package com.atguigu.realtime.util;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.runtime.typeutils.StringDataSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;


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
                .setProperty("isolation.level", "read_committed ")
//                .setValueOnlyDeserializer(new SimpleStringSchema())   // 存在问题，当kafka的值为null时无法正常解析
//                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserializationSchema<String>() {
                    // 是否结束流
                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    // 处理kafka的数据
                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        byte[] value = record.value();
                        if (value != null) {
                            return new String(value, StandardCharsets.UTF_8);
                        }
                        return null;
                    }

                    // 反序列化后的数据类型
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                }))
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
