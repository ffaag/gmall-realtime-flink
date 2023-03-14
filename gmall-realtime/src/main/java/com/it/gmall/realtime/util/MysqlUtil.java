package com.it.gmall.realtime.util;

/**
 * @author ZuYingFang
 * @time 2023-03-12 17:53
 * @description 读取MySQL的 base_dic 表作为LookUp表base_dic，也就是将其作为维表，维度退化和主表Join
 */
public class MysqlUtil {


    /**
     * 如果在 Flink SQL 表的 DDL 语句中定义了主键，则会以 upsert 模式将流中数据写入数据库，
     * 此时流中可以存在 UPDATE/DElETE（更新/删除）类型的数据。否则，会以 append 模式将数据写出到数据库，
     * 此时流中只能有 INSERT（插入）类型的数据
     * 具体格式见文档第九章交易域加购事务事实表
     */
    public static String getBaseDicLookUpDDL() {
        return "create table `base_dic`( " +
                "`dic_code` string, " +
                "`dic_name` string, " +
                "`parent_code` string, " +
                "`create_time` timestamp, " +
                "`operate_time` timestamp, " +
                "primary key(`dic_code`) not enforced " +
                ")" + MysqlUtil.mysqlLookUpTableDDL("base_dic");
    }


    /**
     * JDBC 连接器可以作为时态表关联中的查询数据源（又称维表）。目前，仅支持同步查询模式。
     * 	默认情况下，查询缓存（Lookup Cache）未被启用，需要设置 lookup.cache.max-rows 和 lookup.cache.ttl 参数来启用此功能。
     * 	Lookup 缓存是用来提升有 JDBC 连接器参与的时态关联性能的。默认情况下，缓存未启用，所有的请求会被发送到外部数据库。
     * 	当缓存启用时，每个进程（即 TaskManager）维护一份缓存。收到请求时，Flink 会先查询缓存，如果缓存未命中才会向外部数据库发送请求，
     * 	并用查询结果更新缓存。如果缓存中的记录条数达到了 lookup.cache.max-rows 规定的最大行数时将清除存活时间最久的记录。
     * 	如果缓存中的记录存活时间超过了 lookup.cache.ttl 规定的最大存活时间，同样会被清除。
     * 	缓存中的记录未必是最新的，可以将 lookup.cache.ttl 设置为一个更小的值来获得时效性更好的数据，
     * 	但这样做会增加发送到数据库的请求数量。所以需要在吞吐量和正确性之间寻求平衡。
     * 	具体格式见文档第九章交易域加购事务事实表
     */
    public static String mysqlLookUpTableDDL(String tableName) {

        return " WITH ( " +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://hadoop102:3306/gmall-211126-flink', " +
                "'table-name' = '" + tableName + "', " +
                "'lookup.cache.max-rows' = '10', " +
                "'lookup.cache.ttl' = '1 hour', " +
                "'username' = 'root', " +
                "'password' = '000000', " +
                "'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
    }

}
