import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class TestJoin1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 设置ttl
        tEnv
                .getConfig()
                .setIdleStateRetention(Duration.ofSeconds(10));

        tEnv
                .executeSql("create table t1(" +
                        "id string,name string" +
                        ")WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 's1'," +
                        "  'properties.bootstrap.servers' = 'hadoop102:9092'," +
                        "  'properties.group.id' = 'atguigu'," +
                        "  'format' = 'csv'" +
                        ")");
        tEnv
                .executeSql("create table t2(" +
                        "id string,age int" +
                        ")WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 's2'," +
                        "  'properties.bootstrap.servers' = 'hadoop102:9092'," +
                        "  'properties.group.id' = 'atguigu'," +
                        "  'format' = 'csv'" +
                        ")");
        Table result = tEnv.sqlQuery("select " +
                "t1.id,t1.name,t2.age " +
                "from t1 left join t2 on t1.id=t2.id");
        tEnv.executeSql("CREATE TABLE t12 (" +
                "  id STRING," +
                "  name STRING," +
                "  age INT," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = 's3'," +
                "  'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "  'key.format' = 'json'," +
                "  'value.format' = 'json'" +
                ")");
        result.executeInsert("t12");
    }
}
