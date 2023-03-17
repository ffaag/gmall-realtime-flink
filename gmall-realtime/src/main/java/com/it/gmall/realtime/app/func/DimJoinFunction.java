package com.it.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @author ZuYingFang
 * @time 2023-03-17 19:00
 * @description
 */
public interface DimJoinFunction<T> {

    String getKey(T input);

    void join(T input, JSONObject dimInfo);
}
