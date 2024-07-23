import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestJoin2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 3000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        /*
        tEnv.executeSql("create table s3(" +
                "id string, " +
                "name string," +
                " age int, " +
                "primary key(id) not enforced" +
                ")with(" +
                " 'connector'='upsert-kafka', " +
                " 'properties.bootstrap.servers'='hadoop102:9092', " +
                " 'topic'='s3', " +
                " 'key.format'='json', " +
                " 'value.format'='json' " +
                ")");
         */

        tEnv.executeSql("create table s3(" +
                "id string, " +
                "name string," +
                " age int " +
                ")with(" +
                " 'connector'='kafka', " +
                " 'properties.bootstrap.servers'='hadoop102:9092', " +
                " 'properties.group.id'='atguigu', " +
                " 'topic'='s3', " +
                " 'format'='json' " +
                ")");

        tEnv.sqlQuery("select * from s3").execute().print();
    }
}
