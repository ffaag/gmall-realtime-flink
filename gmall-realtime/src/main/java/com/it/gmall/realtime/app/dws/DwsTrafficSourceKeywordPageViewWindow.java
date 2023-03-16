package com.it.gmall.realtime.app.dws;

import com.it.gmall.realtime.app.func.SplitFunction;
import com.it.gmall.realtime.bean.KeywordBean;
import com.it.gmall.realtime.util.KafkaUtil;
import com.it.gmall.realtime.util.MyClickHouseUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author ZuYingFang
 * @time 2023-03-14 10:47
 * @description 流量域来源关键词粒度页面浏览各窗口汇总表（FlinkSQL）。
 * 从 Kafka 页面浏览明细主题读取数据，过滤搜索行为，使用自定义 UDTF（一进多出）函数对搜索内容分词。统计各窗口各关键词出现频次，写入 ClickHouse
 * 数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
 * 程  序：     Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwsTrafficSourceKeywordPageViewWindow > ClickHouse(ZK)
 */
public class DwsTrafficSourceKeywordPageViewWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);    //生产环境中设置为Kafka主题的分区数
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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


        //TODO 2.使用DDL方式读取Kafka page_log 主题的数据创建表并且提取时间戳生成Watermark
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window";
        tableEnv.executeSql("" +
                "create table page_log( " +
                "    `page` map<string,string>, " +
                "    `ts` bigint, " +
                "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +  // 里面的函数将数字转为特定格式的时间字符串，外面的函数将其转为时间戳
                "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +  // 设置水位线，等待两秒
                " ) " + KafkaUtil.getKafkaDDL(topic, groupId));


        //TODO 3.过滤出搜索数据
        Table filterTable = tableEnv.sqlQuery("" +
                " select " +
                "    page['item'] item, " +
                "    rt " +
                " from page_log " +
                " where page['last_page_id'] = 'search' " +
                " and page['item_type'] = 'keyword' " +
                " and page['item'] is not null");
        tableEnv.createTemporaryView("filter_table", filterTable);


        //TODO 4.注册UDTF & 切词
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);  // 注册函数
        Table splitTable = tableEnv.sqlQuery("" +
                "SELECT " +
                "    word, " +
                "    rt " +
                "FROM filter_table,  " +
                "LATERAL TABLE(SplitFunction(item))");   // 炸裂函数，将切分的字符串炸裂开来，一个个放到word里面，一行变多行
        tableEnv.createTemporaryView("split_table", splitTable);
        tableEnv.toAppendStream(splitTable, Row.class).print("Split>>>>>>");


        //TODO 5.分组、开窗、聚合
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    word keyword, " +   // 炸裂开来的搜索词
                "    count(*) keyword_count, " +   // 开窗后聚合统计的个数
                "    UNIX_TIMESTAMP()*1000 ts " +   // 将时间戳转换为毫秒
                "from split_table " +
                "group by word,TUMBLE(rt, INTERVAL '10' SECOND)");  // 开一个10s滚动窗口

        //TODO 6.将动态表转换为流
        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(resultTable, KeywordBean.class);


        //TODO 7.将数据写出到ClickHouse
        keywordBeanDataStream.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));

        //TODO 8.启动任务
        env.execute("DwsTrafficSourceKeywordPageViewWindow");

    }

}
