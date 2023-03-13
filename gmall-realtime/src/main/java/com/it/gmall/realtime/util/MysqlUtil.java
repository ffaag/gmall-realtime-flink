package com.it.gmall.realtime.util;

/**
 * @author ZuYingFang
 * @time 2023-03-12 17:53
 * @description 读取MySQL的 base_dic 表作为LookUp表base_dic，也就是将其作为维表，维度退化和主表Join
 */
public class MysqlUtil {

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
