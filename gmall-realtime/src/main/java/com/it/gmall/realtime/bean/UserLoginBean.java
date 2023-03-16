package com.it.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ZuYingFang
 * @time 2023-03-16 14:56
 * @description 给DwsUserUserLoginWindow用
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserLoginBean {


    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 回流用户数
    Long backCt;

    // 独立用户数
    Long uuCt;

    // 时间戳
    Long ts;


}
