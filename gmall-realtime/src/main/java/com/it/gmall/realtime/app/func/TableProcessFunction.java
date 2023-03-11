package com.it.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.it.gmall.realtime.bean.TableProcess;
import com.it.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @author ZuYingFang
 * @time 2023-03-07 21:09
 * @description DimApp中，两条流合并后需要处理的逻辑
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;
    MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_DRIVER);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    /**
     * 根据广播流中存储的配置表的数据，在phoenix中建立对应的维表并将数据存储进去，然后再把原表名和配置表的信息pojo对象存到广播流的描述器中
     * 主流就可以根据这个描述器得到哪些数据要存到phoenix中，根据对应的数据知道要保存的列都是哪些
     *
     * @param value 广播流
     * @param ctx   上下文
     * @param out   收集器
     * @throws Exception value:{"before":null,"after":{"source_table":"aa","sink_table":"bb","sink_columns":"cc","sink_pk":"id","sink_extend":"xxx"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1652513039549,"snapshot":"false","db":"gmall-211126-config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1652513039551,"transaction":null}
     */
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        //1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess after = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.校验并建表
        checkTable(after.getSinkTable(), after.getSinkColumns(), after.getSinkPk(), after.getSinkExtend());

        //3.写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(after.getSourceTable(), after);

    }

    /**
     * 校验并建表:create table if not exists db.tn(id varchar primary key,bb varchar,cc varchar) xxx，建立phoenix表格
     *
     * @param sinkTable   Phoenix表名
     * @param sinkColumns Phoenix表字段
     * @param sinkPk      Phoenix表主键
     * @param sinkExtend  Phoenix表扩展字段
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            //处理特殊字段
            if (sinkPk == null || sinkPk.equals("")) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuilder append = new StringBuilder("create table if not exists ").append(GmallConfig.HBASE_SCHEMA).append(".").append(sinkTable).append("(");

            String[] split = sinkColumns.split(",");

            for (int i = 0; i < split.length; i++) {
                String s = split[i];
                //判断是否为主键
                if (s.equals(sinkPk)) {
                    append.append(sinkPk).append(" varchar primary key");
                } else {
                    append.append(s).append(" varchar");
                }

                if (i < split.length - 1) {
                    //判断是否为最后一个字段
                    append.append(",");
                }
            }
            append.append(")").append(sinkExtend);

            //编译SQL
            preparedStatement = connection.prepareStatement(append.toString());

            //执行SQL,建表
            preparedStatement.execute();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表失败：" + sinkTable);
        } finally {
            // 释放资源
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
    }


    /** 主流根据广播流状态描述器中存储的pojo对象，过滤字段，只保存要保存的那几列，将其put方法写到流中，打个标签就可以了
     * @param value
     * @param ctx
     * @param out
     * @throws Exception value:{"database":"gmall-211126-flink","table":"base_trademark","type":"update","ts":1652499176,"xid":188,"commit":true,"data":{"id":13,"tm_name":"atguigu","logo_url":"/bbb/bbb"},"old":{"logo_url":"/aaa/aaa"}}
     *                   value:{"database":"gmall-211126-flink","table":"order_info","type":"update","ts":1652499176,"xid":188,"commit":true,"data":{"id":13,...},"old":{"xxx":"/aaa/aaa"}}
     */
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //1.获取广播中的的配置数据，也就是得到要存进phoenix表的tableProcess对象
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(value.getString("table"));

        //2 过滤字段,只存维度表，事实表因为tableProcess里面没有就不会存到hbase里面
        if (tableProcess != null) {
            filterColume(value.getJSONObject("data"), tableProcess.getSinkColumns());

            //3.补充SinkTable并写出到流中，这个时候已经修改完了data，且我们将数据要存的hbase表明放进去
            value.put("sinkTable", tableProcess.getSinkTable());
            out.collect(value);

        } else {
            System.out.println("找不到对应的Key：" + tableProcess.getSourceTable());
        }


    }


    /**
     * 过滤字段
     *
     * @param data        {"id":13,"tm_name":"atguigu","logo_url":"/bbb/bbb"}
     * @param sinkColumns "id,tm_name"
     */
    private void filterColume(JSONObject data, String sinkColumns) {

//        问题：data:{"id":"1001","name":"aaa","tm_name":"bbb"}
//              sinkColumns:"id,tm_name"   这样的话，还是都包含进去了
//        Set<Map.Entry<String, Object>> entries = data.entrySet();
//        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if (!sinkColumns.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }

        String[] split = sinkColumns.split(",");
        List<String> stringList = Arrays.asList(split);
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !stringList.contains(next.getKey()));  // 这就是上面代码的简写，直接在上面的语句alt+enter

    }
}