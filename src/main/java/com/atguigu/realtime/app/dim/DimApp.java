package com.atguigu.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV1;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.common.Constant;
import com.atguigu.realtime.util.FlinkJdbcUtil;
import com.atguigu.realtime.util.FlinkSinkUtil;
import com.atguigu.realtime.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

public class DimApp extends BaseAppV1 {


    public static void main(String[] args) {
        new DimApp().initKafka(2000, 2, "DimApp", Constant.TOPIC_ODS_DB);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 数据ETL
        SingleOutputStreamOperator<JSONObject> etlEdStream = etl(stream);
        // 2. 读取配置表
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);
        // 3. 数据流和广播流connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dataTpConnectedStream = connectStream(etlEdStream, tpStream);
        // 4. 过滤掉dim层不需要的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultDimStream = filterColumns(dataTpConnectedStream);
        // 5. 数据写入phoenix
        writePhoenix(resultDimStream);
    }

    /*
    JdbcSink只能写入一张表，无法满足
    自定义Sink
     */
    private void writePhoenix(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        stream.addSink(FlinkSinkUtil.getPhoenixSink());
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dataTpConnectedStream) {
        return dataTpConnectedStream
                .map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> value) throws Exception {
                        JSONObject data = value.f0;
                        List<String> phoenixColumns = Arrays.asList(value.f1.getSinkColumns().split(","));
                        // data 本质时一个map，删除不需要的键值对，保留自定义的ods_db_type字段
                        data.keySet().removeIf(key -> !phoenixColumns.contains(key) && !"ods_db_type".equals(key));
                        return value;
                    }
                });
    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStream(SingleOutputStreamOperator<JSONObject> dataStream, SingleOutputStreamOperator<TableProcess> tpStream) {
        MapStateDescriptor<String, TableProcess> tpBroadcastStateDesc;
        // 0. 根据配置信息建phoenix表，在做成广播流前建，否则，广播流会把数据发送到每一个并行度的流当中，有几个并行度就会建几次表
        tpStream = tpStream.map(new RichMapFunction<TableProcess, TableProcess>() {
            private Connection conn;

            @Override
            public void open(Configuration parameters) throws Exception {
                conn = FlinkJdbcUtil.getPhoenixConnection();
            }

            @Override
            public void close() throws Exception {
                FlinkJdbcUtil.closePhoenixConnection(conn);
            }

            @Override
            public TableProcess map(TableProcess value) throws Exception {
                // 避免长时间未使用导致连接关闭
                if (conn.isClosed()) {
                    conn = FlinkJdbcUtil.getPhoenixConnection();
                }
                StringBuilder sql = new StringBuilder();
                sql
                        .append("create table if not exists ")
                        .append(value.getSinkTable())
                        .append("(")
                        .append(value.getSinkColumns().replaceAll("[^,]+", "$0 varchar"))
                        .append(", constraint pk primary key(")
                        .append(value.getSinkPk() == null ? "id" : value.getSinkPk())
                        .append("))")
                        .append(value.getSinkExtend() == null ? "" : value.getSinkExtend());
                PreparedStatement ps = conn.prepareStatement(sql.toString());
                System.out.println("phoenix建表语句" + sql.toString());
                ps.execute();
                ps.close();
                return value;
            }
        });
        // 1. 配置表做成广播流
        tpBroadcastStateDesc = new MapStateDescriptor<>("tpBroadcastState", TypeInformation.of(String.class), TypeInformation.of(TableProcess.class));
        BroadcastStream<TableProcess> tpBCStream = tpStream.broadcast(tpBroadcastStateDesc);
        // 2. 数据流connect广播流
        return dataStream
                .connect(tpBCStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        // 4. 处理数据流的数据时，从广播状态获取配置信息
                        ReadOnlyBroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpBroadcastStateDesc);
                        TableProcess tp = state.get(value.getString("table"));
                        // 如果不是维度表或者时不需要sink的维度表，tp为null
                        if (tp != null) {
                            JSONObject jObj = value.getJSONObject("data");
                            jObj.put("ods_db_type", value.getString("type"));
                            out.collect(Tuple2.of(jObj, tp));
                        }
                    }

                    @Override
                    public void processBroadcastElement(TableProcess value, BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>.Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        // 3. 配置信息写入广播状态
                        BroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpBroadcastStateDesc);
                        String sourceTable = value.getSourceTable();
                        state.put(sourceTable, value);
                    }
                });


    }

    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        return env
                .fromSource(FlinkSourceUtil.getMysqlSource("gmall_config", "gmall_config.table_process"), WatermarkStrategy.noWatermarks(), "Mysql Source")
                .map(json -> {
                    JSONObject jsonObject = JSON.parseObject(json);
                    return jsonObject.getObject("after", TableProcess.class);
                });
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                // etl
                .filter(json -> {
                    try {
                        // 临时将 bootstrap-insert 修改为insert
                        JSONObject jsonObject = JSON.parseObject(json.replaceAll("bootstrap-", ""));
                        return "gmall".equals(jsonObject.getString("database"))
                                && ("insert".equals(jsonObject.getString("type")) || "update".equals(jsonObject.getString("type")))
                                && jsonObject.getString("data") != null
                                && jsonObject.getString("data").length() > 2;
                    } catch (Exception e) {
                        System.out.println("非json格式" + json);
                        return false;
                    }
                })
                // 转为jsonObject
                .map(JSON::parseObject);
    }
}
