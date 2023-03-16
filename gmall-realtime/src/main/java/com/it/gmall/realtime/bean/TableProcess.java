package com.it.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ZuYingFang
 * @time 2023-03-07 20:58
 * @description DimApp中配置流转为广播流后，值里面存储的内容
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcess {

    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;


}
