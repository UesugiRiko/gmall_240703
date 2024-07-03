package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


/**
 * 无法满足特殊清况
 * home页->关闭->home页->关闭->home页
 */
public class DWD03DwdTrafficUserJumpDetailFun1 extends BaseAppV1 {
    public static void main(String[] args) {
        new DWD03DwdTrafficUserJumpDetailFun1().initKafka(2103, 2, "DWD03DwdTrafficUserJumpDetail", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 测试数据
        stream = env
                .fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":11000} ",
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"home\"},\"ts\":17000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"home\"},\"ts\":18000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":\"detail\"},\"ts\":30000} "
                );
        KeyedStream<JSONObject, String> keyedStream = stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((obj, ts) -> obj.getLong("ts")))
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"));
        // 1. 定义模式
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("entry")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String last_page_id = value.getJSONObject("page").getString("last_page_id");
                        return last_page_id == null || last_page_id.length() == 0;
                    }
                })
                .next("second")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String last_page_id = value.getJSONObject("page").getString("last_page_id");
                        return last_page_id != null && last_page_id.length() > 0;
                    }
                })
                .within(Time.seconds(5));
        // 2. 模式应用于流
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);
        // 3. 获取数据
        SingleOutputStreamOperator<String> normal = patternStream.select(
                new OutputTag<String>("timeout") {
                },
                new PatternTimeoutFunction<JSONObject, String>() {
                    @Override
                    public String timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                        return pattern.get("entry").get(0).toJSONString();
                    }
                },
                // 不需要正常数据，不做处理
                new PatternSelectFunction<JSONObject, String>() {
                    @Override
                    public String select(Map<String, List<JSONObject>> pattern) throws Exception {
                        return null;
                    }
                }
        );
        normal.getSideOutput(new OutputTag<String>("timeout") {
        }).print();
    }
}
