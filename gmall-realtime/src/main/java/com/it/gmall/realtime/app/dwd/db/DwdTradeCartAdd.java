package com.it.gmall.realtime.app.dwd.db;

import com.it.gmall.realtime.util.KafkaUtil;
import com.it.gmall.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author ZuYingFang
 * @time 2023-03-12 15:59
 * @description 交易域加购事务事实表。首先读取topic_db中的所有数据，存到表格topic_db中，然后查询topic_db的数据，找到cart_info这张表格的数据
 * 接着读取MySQL的 base_dic表作为LookUp表base_dic，关联两张表，将关联得到的数据写入新创建的表格dwd_cart_add中
 * 这个新创建的表格dwd_cart_add的数据是存储在kafka中的，他是连接着kafka的dwd_trade_cart_add主题
 * 数据流：Web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
 * 程  序：Mock  ->  Mysql  ->  Maxwell -> Kafka(ZK)  ->  DwdTradeCartAdd -> Kafka(ZK)
 */
public class DwdTradeCartAdd {

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


        //TODO 2.使用DDL方式读取 topic_db 主题的数据创建表
        tableEnv.executeSql(KafkaUtil.getTopicDb("cart_add"));


        //TODO 3.过滤出加购数据
        Table cartAddTable = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] id, " +
                "    `data`['user_id'] user_id, " +
                "    `data`['sku_id'] sku_id, " +
                "    `data`['cart_price'] cart_price, " +
                "    if(`type`='insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num, " +
                "    `data`['sku_name'] sku_name, " +
                "    `data`['is_checked'] is_checked, " +
                "    `data`['create_time'] create_time, " +
                "    `data`['operate_time'] operate_time, " +
                "    `data`['is_ordered'] is_ordered, " +
                "    `data`['order_time'] order_time, " +
                "    `data`['source_type'] source_type, " +
                "    `data`['source_id'] source_id, " +
                "    pt " +
                "from topic_db " +
                "where `database` = 'gmall-211126-flink' " +
                "and `table` = 'cart_info' " +
                "and (`type` = 'insert' " +
                "or (`type` = 'update'  " +
                "    and  " +
                "    `old`['sku_num'] is not null  " +
                "    and  " +
                "    cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))");
        tableEnv.createTemporaryView("cart_info_table", cartAddTable);


        //TODO 4.读取MySQL的 base_dic 表作为LookUp表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());


        //TODO 5.关联两张表
        /**
         * Lookup Join 通常在 Flink SQL 表和外部系统查询结果关联时使用。这种关联要求一张表（主表）有处理时间字段，
         * 而另一张表（维表）由 Lookup 连接器生成。
         * 	Lookup Join 做的是维度关联，而维度数据是有时效性的，那么我们就需要一个时间字段来对数据的版本进行标识。
         * 	因此，Flink 要求我们提供处理时间用作版本字段。此处选择在topic_db表中调用 PROCTIME() 函数获取系统时间，将其作为处理时间字段。
         */
        Table cartAddWithDicTable = tableEnv.sqlQuery("" +
                "select " +
                "    ci.id, " +
                "    ci.user_id, " +
                "    ci.sku_id, " +
                "    ci.cart_price, " +
                "    ci.sku_num, " +
                "    ci.sku_name, " +
                "    ci.is_checked, " +
                "    ci.create_time, " +
                "    ci.operate_time, " +
                "    ci.is_ordered, " +
                "    ci.order_time, " +
                "    ci.source_type source_type_id, " +
                "    dic.dic_name source_type_name, " +
                "    ci.source_id " +
                "from cart_info_table ci " +
                "join base_dic FOR SYSTEM_TIME AS OF ci.pt as dic " +
                "on ci.source_type = dic.dic_code");
        tableEnv.createTemporaryView("cart_add_dic_table", cartAddWithDicTable);


        //TODO 6.使用DDL方式创建加购事实表
        tableEnv.executeSql("" +
                "create table dwd_cart_add( " +
                "    `id` STRING, " +
                "    `user_id` STRING, " +
                "    `sku_id` STRING, " +
                "    `cart_price` STRING, " +
                "    `sku_num` STRING, " +
                "    `sku_name` STRING, " +
                "    `is_checked` STRING, " +
                "    `create_time` STRING, " +
                "    `operate_time` STRING, " +
                "    `is_ordered` STRING, " +
                "    `order_time` STRING, " +
                "    `source_type_id` STRING, " +
                "    `source_type_name` STRING, " +
                "    `source_id` STRING " +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));


        //TODO 7.将数据写出
        tableEnv.executeSql("insert into dwd_cart_add select * from cart_add_dic_table");


        //TODO 8.启动任务
        env.execute("DwdTradeCartAdd");


    }

}
