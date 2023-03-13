package com.it.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.it.gmall.realtime.util.DateFormatUtil;
import com.it.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author ZuYingFang
 * @time 2023-03-10 18:10
 * @description 流量域独立访客事务事实表。过滤页面数据中的独立访客访问记录，即将用户首日访问的记录输出到特定topic的kafka流中，当日其他的访问记录丢弃掉
 * 数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
 * 程  序：Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUniqueVisitorDetail -> Kafka(ZK)
 */
public class DwdTrafficUniqueVisitorDetail {

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


        //TODO 2.读取Kafka 页面日志主题创建流
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer("dwd_traffic_page_log", "dwd_traffic_page_log"));

        //TODO 3.过滤掉上一跳页面不为null的数据并将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    //获取上一跳页面ID
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(value);
                }

            }
        });

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        //TODO 5.使用状态编程实现按照Mid的去重
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);

                // 设置状态存活时间TTL为一天
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                stringValueStateDescriptor.enableTimeToLive(ttlConfig);

                lastVisitState = getRuntimeContext().getState(stringValueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //获取状态数据&当前数据中的时间戳并转换为日期
                Long ts = value.getLong("ts");
                String lastDate = lastVisitState.value();
                String curDate = DateFormatUtil.toDate(ts);

                // 当前状态为空或者当前时间与状态里面的时间不是一天，则认为其是当日第一次访问，保存此条数据
                if (lastDate == null || !lastDate.equals(curDate)) {
                    return true;
                }
                return false;
            }
        });


        //TODO 6.将数据写到Kafka
        uvDS.map(json -> json.toJSONString()).addSink(KafkaUtil.getFlinkKafkaProducer("dwd_traffic_unique_visitor_detail"));


        //TODO 7.启动任务
        env.execute("DwdTrafficUniqueVisitorDetail");
    }

}
