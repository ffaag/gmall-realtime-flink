package com.it.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.it.gmall.realtime.app.func.DimSinkFunction;
import com.it.gmall.realtime.app.func.TableProcessFunction;
import com.it.gmall.realtime.bean.TableProcess;
import com.it.gmall.realtime.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ZuYingFang
 * @time 2023-03-07 19:25
 * @description 数据流：web/app -> nginx -> 业务服务器 -> Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Phoenix
 * 程  序：              Mock -> Mysql(binlog) -> Maxwell -> Kafka(ZK) -> DimApp(FlinkCDC/Mysql) -> Phoenix(HBase/ZK/HDFS)
 * 分别创建主流和广播流，广播流在hbase phoenix中创建配置表，主流根据广播流的配置表创建维表存在hbase里面
 */
public class DimApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境中设置为Kafka主题的分区数

//        //1.1 开启CheckPoint
//        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
//
//        //1.2 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/checkpoint");
//        System.setProperty("HADOOP_USER_NAME", "xiaofang");


        //TODO 2.读取 Kafka topic_db 主题数据创建主流
        String topic = "topic_db";
        String groupId = "dim_app";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));


        //TODO 3.过滤掉非JSON数据&保留新增、变化以及初始化数据并将数据转换为JSON格式
        SingleOutputStreamOperator<JSONObject> filterJsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String type = jsonObject.getString("type");
                    if (type.equals("insert") || type.equals("update") || type.equals("bootstrap-insert")) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("脏数据" + value);
                }
            }
        });


        //TODO 4.使用FlinkCDC读取MySQL配置信息表创建配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("fangzu")
                .databaseList("gmall-config")
                .tableList("gmall-config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MysqlSource");


        //TODO 5.将配置流处理为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlSourceDS.broadcast(mapStateDescriptor);


        //TODO 6.连接主流与广播流   //TODO 7.处理连接流,根据配置信息处理主流数据
        // 将广播流广播，主流根据广播流的要求已经修改好data数据，也就是只剩下广播流配置表中的字段，现在只剩下把数据存进对应的维表中
        SingleOutputStreamOperator<JSONObject> process = filterJsonObjDS.connect(broadcastStream).process(new TableProcessFunction(mapStateDescriptor));


        //TODO 8.将数据写出到Phoenix
        process.addSink(new DimSinkFunction());

        //TODO 9.启动任务
        env.execute("DimApp");


    }
}