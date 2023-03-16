package com.it.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ZuYingFang
 * @time 2023-03-14 15:06
 * @description DwsTrafficSourceKeywordPageViewWindow.java中查询得到的结果转为一个java bean
 * 匹配是通过列名而不是顺序，所以要保证列名一致
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordBean {

    // 窗口起始时间
    private String stt;
    // 窗口闭合时间
    private String edt;
    // 关键词来源   ---  辅助字段,不需要写入ClickHouse
    // @TransientSink  //自定义注解
    private String source;
    // 关键词
    private String keyword;
    // 关键词出现频次
    private Long keyword_count;
    // 时间戳
    private Long ts;

}
