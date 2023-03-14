package com.it.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.it.gmall.realtime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
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
 * @author ZuYingFang
 * @time 2023-03-10 19:29
 * @description 流量域用户跳出事务事实表。
 * 数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
 * 程  序：Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUserJumpDetail -> Kafka(ZK)
 * 过滤用户跳出明细数据，即只访问一个页面的，使用flink CEP实现，要么连续两个lastPage为空的，要么一个为空后面超时的，将其得到后联合流，写入kafka中
 */
public class DwdTrafficUserJumpDetail {

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
//        System.setProperty("HADOOP_USER_NAME", "xiaofang");、

        //TODO 2.读取Kafka 页面日志主题创建流
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer("dwd_traffic_page_log", "user_jump_detail"));

        //TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 4.提取事件时间&按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                })).keyBy(json -> json.getJSONObject("common").getString("mid"));


        //TODO 5.定义CEP的模式序列，即匹配第一个上一页为空，第二个上一页也为空的数据，而这就是用户跳出数据，这只是一个模式
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));  // 如果第一个为空，第二个很长时间没有到来怎么办呢，就设置了超时时间为10s，超过10s还没有第二条则认定第一条也符合过滤条件

//        Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                return value.getJSONObject("page").getString("last_page_id") == null;
//            }
//        })
//                .times(2)      //默认是宽松近邻 followedBy
//                .consecutive() //严格近邻 next
//                .within(Time.seconds(10));

        //TODO 6.将模式序列作用到流上，即把上面我们定义的模式作用到当前的主流上，这样就得到了符合条件的数据了
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7.提取事件(匹配上的事件以及超时事件)，成功匹配的写入主流，超时时间导致的会写入到侧输出流，因此先搞一个侧输出流
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut") {
        };
        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, String>() {
            // 这是超时时间的，将其输入到侧输出流中
            @Override
            public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0).toJSONString();
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            // 这是正常的前后两条数据的lastPage都为空，所以将第一条数据保存
            @Override
            public String select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0).toJSONString();
            }
        });

        // 这就是侧输出流中的数据
        DataStream<String> timeOutDS = selectDS.getSideOutput(timeOutTag);

        //TODO 8.合并两个种事件
        DataStream<String> unionDS = selectDS.union(timeOutDS);

        //TODO 9.将数据写出到Kafka
//        selectDS.print("Select>>>>>>>");
//        timeOutDS.print("TimeOut>>>>>");
        String targetTopic = "dwd_traffic_user_jump_detail";
        unionDS.addSink(KafkaUtil.getFlinkKafkaProducer(targetTopic));

        //TODO 10.启动任务
        env.execute("DwdTrafficUserJumpDetail");

    }

}
