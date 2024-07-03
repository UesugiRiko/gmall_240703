package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSinkUtil;
import com.atguigu.realtime.util.SelfUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 开窗有问题还得在测
 * 怀疑是否是需要增加判断状态不为空
 * 用状态解决
 */
// TODO: 2024/7/2
public class DWD02DwdTrafficUniqueVisitDetail extends BaseAppV1 {
    public static void main(String[] args) {
        new DWD02DwdTrafficUniqueVisitDetail().initKafka(2102, 2, "DWD02DwdTrafficUniqueVisitDetail", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream
                .map(JSON::parseObject)
                /*
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((obj, ts) -> {
                    Long teGet = obj.getLong("ts");
                    String uid = obj.getJSONObject("common").getString("uid");
                    String formatDt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(teGet));
                    System.out.println(uid+"====="+SelfUtil.LongToDate(teGet));
                    System.out.println(uid+"====="+formatDt);
                    return teGet;
                }))
                */
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((obj, ts) -> obj.getLong("ts")))
                .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, String>() {

                    private ValueState<String> firstVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDateStateDesc", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        String firstVisitDate = firstVisitDateState.value();
                        String today = SelfUtil.LongToDate(value.getLong("ts"));
                        if (!today.equals(firstVisitDate)) {
                            firstVisitDateState.update(today);
                            out.collect(value.toJSONString());
                        }
                    }
                })
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_UV_DETAIL));
//                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
//                .process(new ProcessWindowFunction<JSONObject, String, String, TimeWindow>() {
//                    @Override
//                    public void process(String s, ProcessWindowFunction<JSONObject, String, String, TimeWindow>.Context context, Iterable<JSONObject> elements, Collector<String> out) throws Exception {
//                        long windowStart = context.window().getStart();
//                        long windowEnd = context.window().getEnd();
//                        String wsDt = SelfUtil.LongToDateTime(windowStart);
//                        String weDt = SelfUtil.LongToDateTime(windowEnd);
//                        int count = 0;
//                        for (JSONObject element : elements) {
//                            count++;
//                            System.out.println("Element in window: " + element);
//                        }
//                        // 打印水印信息
//                        System.out.println("Current watermark: " + context.currentWatermark());
//
//                        // 打印窗口开始和结束时间
//                        System.out.println("Window Start: " + wsDt);
//                        System.out.println("Window End: " + weDt);
//
//                        // 打印窗口内元素数量
//                        System.out.println("Window [" + wsDt + " - " + weDt + "] has " + count + " elements.");
//                        if (count > 0) {
//                            out.collect(s + "===" + wsDt + "=====" + weDt + ", Count: " + count);
//                        } else {
//                            System.out.println("No elements in window: " + wsDt + " - " + weDt);
//                        }
//                    }
//                })
//                .process(new ProcessWindowFunction<JSONObject, String, String, TimeWindow>() {
//
//                    private ValueState<String> visitDateState;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        visitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("visitDateStateDesc", String.class));
//                    }
//
//                    @Override
//                    public void process(String s, ProcessWindowFunction<JSONObject, String, String, TimeWindow>.Context context, Iterable<JSONObject> elements, Collector<String> out) throws Exception {
//
//                        //找到当天的第一个窗口
//                        String date = visitDateState.value();
//                        String today = SelfUtil.LongToDate(context.window().getStart());
//                        if (!today.equals(date)) {
//                            List<JSONObject> list = SelfUtil.iterableToList(elements);
//                            JSONObject min = Collections.min(list, Comparing(o->o.getLong("ts")));
//                            out.collect(min.toJSONString());
//                            visitDateState.update(today);
//                        }
//                    }
//                })
//                .print();
//                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_UV_DETAIL));

    }
}
