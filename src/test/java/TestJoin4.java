import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class TestJoin4 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        /*
        // lookup join，ttl参数无效
        tEnv
                .getConfig().setIdleStateRetention(Duration.ofSeconds(10));

         */

        tEnv
                .executeSql("CREATE TABLE t1( " +
                        "id STRING," +
                        "pt as proctime()" +
                        ") WITH (" +
                        "'connector' = 'kafka'," +
                        "'topic' = 's1'," +
                        "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                        "'properties.group.id' = 'atguigu'," +
                        "'format' = 'csv'" +
                        ")");
        tEnv
                .executeSql("CREATE TABLE t2(" +
                        "dic_code STRING," +
                        "dic_name STRING" +
                        ") WITH (" +
                        "'connector' = 'jdbc'," +
                        "'url' = 'jdbc:mysql://hadoop102:3306/gmall'," +
                        "'table-name' = 'base_dic'," +
                        "'username' = 'root'," +
                        "'password' = '000000'," +
                        "'lookup.cache.max-rows' = '10'," +
                        "'lookup.cache.ttl' = '30 second'" +
                        ")");
        tEnv
                .sqlQuery("select " +
                        "t1.id,d.dic_name " +
                        "from t1 " +
                        "join t2 FOR SYSTEM_TIME AS OF t1.pt AS d ON t1.id = d.dic_code")
                .execute()
                .print();
    }
}