package com.it.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.it.gmall.realtime.util.DruidDSUtil;
import com.it.gmall.realtime.util.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author ZuYingFang
 * @time 2023-03-08 16:01
 * @description 把处理好的主流维表数据存入hbase
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private DruidDataSource druidDataSource = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }


    //value:{"database":"gmall-211126-flink","table":"base_trademark","type":"update","ts":1652499176,"xid":188,"commit":true,"data":{"id":13,"tm_name":"atguigu"},"old":{"logo_url":"/aaa/aaa"},"sinkTable":"dim_xxx"}
    //value:{"database":"gmall-211126-flink","table":"order_info","type":"update","ts":1652499176,"xid":188,"commit":true,"data":{"id":13,...},"old":{"xxx":"/aaa/aaa"},"sinkTable":"dim_xxx"}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();

        //获取数据类型
        String type = value.getString("type");
        //如果为更新数据,则需要删除Redis中的数据
//        if (type.equals("update")) {
//            DimUtil.delDimInfo(sinkTable.toUpperCase(), data.getString("id"));
//        }

        //写出数据
        PhoenixUtil.upsertValues(connection, value.getString("sinkTable"), value.getJSONObject("data"));

        //归还连接
        connection.close();

    }
}
