package com.atguigu.realtime.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.util.DruidDsUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.PreparedStatement;

public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    public DruidDataSource druidDataSource;
    public DruidPooledConnection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDsUtil.getDruidDataSource();
    }

    @Override
    public void close() throws Exception {
        druidDataSource.close();
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {

        conn = druidDataSource.getConnection();

        JSONObject data = value.f0;
        TableProcess tp = value.f1;
        StringBuilder sql = new StringBuilder();
        sql
                .append("upsert into ")
                .append(tp.getSinkTable())
                .append("(")
                .append(tp.getSinkColumns())
                .append(") values(")
                .append(tp.getSinkColumns().replaceAll("[^,]+", "?"))
                .append(")");
        String[] split = tp.getSinkColumns().split(",");
        PreparedStatement ps = conn.prepareStatement(sql.toString());
        for (int i = 0; i < split.length; i++) {
            String temp = data.get(split[i]) == null ? null : data.get(split[i]).toString();
            ps.setString(i + 1, temp);
        }
        System.out.println(sql.toString());
        ps.execute();
        conn.commit();
        ps.close();
        conn.close();
    }
}