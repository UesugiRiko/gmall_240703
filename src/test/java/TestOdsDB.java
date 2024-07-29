import com.atguigu.realtime.app.BaseSqlApp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TestOdsDB extends BaseSqlApp {
    public static void main(String[] args) {
        new TestOdsDB().initKafka(9900, 2, "TestOdsDB");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        readOdsDBFromEarliest(tEnv, "TestOdsDB");
        tEnv
                .sqlQuery("select " +
                        "`type`, " +
                        "`data`['order_status'], " +
                        "`old`['order_status'] " +
                        "from ods_db " +
                        "where `database` =  'gmall' AND `table` = 'order_info' " +
                        "group by `type`,`data`['order_status'], `old`['order_status']").execute().print();
    }
}
