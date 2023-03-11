package com.it.gmall.realtime.util;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.it.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author ZuYingFang
 * @time 2023-03-08 16:38
 * @description 将主流中的维表数据存进hbase，执行sql语句
 */
public class PhoenixUtil {

    /**
     * @param connection Phoenix连接
     * @param sinkTable  表名   tn
     * @param data       数据   {"id":"1001","name":"zhangsan","sex":"male"}
     */
    public static void upsertValues(DruidPooledConnection connection, String sinkTable, JSONObject data) throws SQLException {
        //1.拼接SQL语句:upsert into db.tn(id,name,sex) values('1001','zhangsan','male')
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();
        //StringUtils.join(columns, ",") == columns.mkString(",")  ==>  id,name,sex
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "("
                + StringUtils.join(keySet, ",") + ") values('" + StringUtils.join(values, "','") + "')";

        //2.预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //3.执行
        preparedStatement.execute();
        connection.commit();

        //4.释放资源
        preparedStatement.close();
    }

}
