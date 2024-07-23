import com.atguigu.realtime.app.BaseAppV1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestJoin3 extends BaseAppV1 {
    public static void main(String[] args) {
        new TestJoin3().initKafka(3000, 2, "TestJoin3", "s3");
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
    }
}
