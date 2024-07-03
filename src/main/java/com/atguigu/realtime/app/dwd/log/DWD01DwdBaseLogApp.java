package com.atguigu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkSinkUtil;
import com.atguigu.realtime.util.SelfUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

public class DWD01DwdBaseLogApp extends BaseAppV1 {

    private final String START = "start";
    private final String PAGE = "page";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";

    public static void main(String[] args) {
        new DWD01DwdBaseLogApp().initKafka(2101, 2, "DWD01DwdBaseLogApp", Constant.TOPIC_ODS_LOG);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. etl
        SingleOutputStreamOperator<JSONObject> etlEdStream = etl(stream);
        // 2. 纠正新老用户标识异常
        SingleOutputStreamOperator<JSONObject> correctedStream = correct(etlEdStream);
        // 3. 数据分流，按信息列别分
        Map<String, DataStream<JSONObject>> shuntedStream = shuntingStream(correctedStream);
//        shuntedStream.get(START).print(START);
//        shuntedStream.get(ERR).print(ERR);
//        shuntedStream.get(DISPLAY).print(DISPLAY);
//        shuntedStream.get(ACTION).print(ACTION);
//        shuntedStream.get(PAGE).print(PAGE);
        // 4. 写入kafka
        writeToKafka(shuntedStream);
    }

    private void writeToKafka(Map<String, DataStream<JSONObject>> stream) {
        stream.get(START).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        stream.get(DISPLAY).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        stream.get(ERR).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        stream.get(ACTION).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        stream.get(PAGE).map(JSONAware::toJSONString).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
    }

    /**
     * 主流：启动
     * 侧输出流：曝光、活动、错误、页面
     *
     * @param stream
     */
    private Map<String, DataStream<JSONObject>> shuntingStream(SingleOutputStreamOperator<JSONObject> stream) {
        OutputTag<JSONObject> displayTag = new OutputTag<>("displayTag", TypeInformation.of(JSONObject.class));
        OutputTag<JSONObject> actionTag = new OutputTag<>("actionTag", TypeInformation.of(JSONObject.class));
        OutputTag<JSONObject> pageTag = new OutputTag<>("pageTag", TypeInformation.of(JSONObject.class));
        OutputTag<JSONObject> errTag = new OutputTag<>("errTag", TypeInformation.of(JSONObject.class));
        SingleOutputStreamOperator<JSONObject> processedStream = stream
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (value.containsKey("start")) {
                            out.collect(value);
                        } else {
                            JSONArray displays = value.getJSONArray("displays");
                            JSONObject common = value.getJSONObject("common");
                            JSONObject page = value.getJSONObject("page");
                            JSONArray actions = value.getJSONArray("actions");
                            Long ts = value.getLong("ts");
                            if (displays != null) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = displays.getJSONObject(i);
                                    display.putAll(common);
                                    display.putAll(page);
                                    display.put("ts", ts);
                                    ctx.output(displayTag, display);
                                }
                                value.remove("displays");
                            }

                            if (actions != null) {
                                for (int i = 0; i < actions.size(); i++) {
                                    JSONObject action = actions.getJSONObject(i);
                                    action.putAll(common);
                                    action.putAll(page);
                                    ctx.output(actionTag, action);
                                }
                                value.remove("actions");
                            }

                            if (value.containsKey("err")) {
                                ctx.output(errTag, value);
                                value.remove("err");
                            }

                            if (page != null) {
                                ctx.output(pageTag, value);
                            }

                        }
                    }
                });
        HashMap<String, DataStream<JSONObject>> resultMap = new HashMap<>();
        resultMap.put(START, processedStream);
        resultMap.put(DISPLAY, processedStream.getSideOutput(displayTag));
        resultMap.put(ERR, processedStream.getSideOutput(errTag));
        resultMap.put(PAGE, processedStream.getSideOutput(pageTag));
        resultMap.put(ACTION, processedStream.getSideOutput(actionTag));
        return resultMap;
    }

    /**
     * 纠正新老标签(mid为判断依据)，问题如下
     * <p>
     * is_new = 1
     * ①状态为null 添加状态
     * ②查看状态日期是否和数据传输的date相同，相同则 is_new = 1
     * ③状态不等，修改is_new = 0
     * <p>
     * is_new = 0
     * 状态为null，状态设置为前一天
     *
     * @param stream
     * @return
     */
    private SingleOutputStreamOperator<JSONObject> correct(SingleOutputStreamOperator<JSONObject> stream) {
        return stream
                .keyBy(x -> x.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> firstVisitDateSate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitDateSate = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDate", String.class));
                    }


                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        String newKey = value.getJSONObject("common").getString("is_new");
                        String todayDate = SelfUtil.LongToDate(value.getLong("ts"));
                        String yesterdayDate = SelfUtil.LongToDate(value.getLong("ts") - 24 * 60 * 60 * 1000);
                        String firstVisitDate = firstVisitDateSate.value();
                        if ("1".equals(newKey)) {
                            if (firstVisitDate == null) {
                                firstVisitDateSate.update(todayDate);
                            } else if (!todayDate.equals(firstVisitDate)) {
                                value.getJSONObject("common").put("is_new", "0");
                            }
                        } else {
                            if (firstVisitDateSate == null) {
                                firstVisitDateSate.update(yesterdayDate);
                            }
                        }
                        return value;
                    }
                });
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        // 只保留json数据
        return stream
                .filter(json -> {
                    try {
                        JSON.parseObject(json);
                    } catch (Exception e) {
                        System.out.println("非json数据" + json);
                        return false;
                    }
                    return true;
                })
                .map(JSON::parseObject);
    }
}
