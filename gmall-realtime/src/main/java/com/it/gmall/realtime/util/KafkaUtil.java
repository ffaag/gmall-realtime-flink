package com.it.gmall.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @author ZuYingFang
 * @time 2023-03-05 16:03
 * @description
 */
public class KafkaUtil {

    static String BOOTSTRAP_SERVERS = "hadoop102:9092, hadoop103:9092, hadoop104:9092";
    static String DEFAULT_TOPIC = "default_topic";

    /**
     * 从kafka读取数据流，与env.addSource相关联
     *
     * @param topic   kafka主题
     * @param groupId kafka消费者组Id
     * @return 但返回一个FlinkKafkaConsumer<String>对象
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            @Override
            // 如果有null则返回null，后续就会过滤掉
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                if (record != null && record.value() != null) {
                    return new String(record.value());
                }
                return null;
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        }, prop);
        return consumer;
    }


    /**
     * 将流数据写入kafka，与stream.addSink相关联
     *
     * @param topic 要写入kafka的主题名
     * @return SinkFunction<String>对象
     */
    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(BOOTSTRAP_SERVERS,
                topic,
                new SimpleStringSchema());
    }

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic, String defaultTopic) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return new FlinkKafkaProducer<String>(defaultTopic,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        if (element == null) {
                            return new ProducerRecord<>(topic, "".getBytes());
                        }
                        return new ProducerRecord<>(topic, element.getBytes());
                    }
                }, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }


    /**
     * topic_db主题的  Kafka-Source DDL 语句，从kafka的topic_db中读取数据并创建表topic_db
     *
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     * 具体格式见文档第九章交易域加购事务事实表
     */
    public static String getTopicDb(String groupId) {
        return "CREATE TABLE topic_db ( " +
                "  `database` STRING, " +
                "  `table` STRING, " +
                "  `type` STRING, " +
                "  `data` MAP<STRING,STRING>, " +
                "  `old` MAP<STRING,STRING>, " +
                "  `pt` AS PROCTIME() " +
                ") " + getKafkaDDL("topic_db", groupId);
    }


    /**
     * Kafka-Source DDL 语句
     *
     * @param topic   数据源主题
     * @param groupId 消费者组
     * @return 拼接好的 Kafka 数据源 DDL 语句
     * 具体格式见文档第九章交易域加购事务事实表
     */
    public static String getKafkaDDL(String topic, String groupId) {
        return " with ('connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'group-offsets')";
    }

    /**
     * Kafka-Sink DDL 语句，将数据表的数据存入kafka的主题中
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 Kafka-Sink DDL 语句
     * 具体格式见文档第九章交易域加购事务事实表
     */
    public static String getKafkaSinkDDL(String topic) {
        return " WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                "  'format' = 'json' " +
                ")";
    }

    /**
     * UpsertKafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题，撤回流，也就是比如左连接，先输入左边数据，此时得到的数据右边为空，
     *              再输入右边数据，此时会撤回第一步连接得到的数据，也就是输出一个null，再输出左右连接的数据
     * @return 拼接好的 UpsertKafka-Sink DDL 语句
     * 具体格式见文档第九章交易域订单预处理表
     *
     * left join 实现过程：假设 A 表作为主表与 B 表做等值左外联。当 A 表数据进入算子，而 B 表数据未至时会先生成一条 B 表字段
     * 均为 null 的关联数据ab1，其标记为 +I。其后，B 表数据到来，会先将之前的数据撤回，即生成一条与 ab1 内容相同，但标记为 -D 的
     * 数据，再生成一条关联后的数据，标记为 +I。这样生成的动态表对应的流称之为回撤流。
     * Kafka SQL Connector 分为Kafka SQL Connector 和 Upsert Kafka SQL Connector：
     * Upsert Kafka Connector支持以 upsert 方式从 Kafka topic 中读写数据
     *
     *
     * 本节需要用到 Kafka 连接器的明细表数据来源于 topic_db 主题，于 Kafka 而言，该主题的数据的操作类型均为 INSERT，
     * 所以读取数据使用 Kafka Connector 即可。而由于 left join 的存在，流中存在修改数据，所以写出数据使用 Upsert Kafka Connector。
     */
    public static String getUpsertKafkaDDL(String topic) {
        return " WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + BOOTSTRAP_SERVERS + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }




}
