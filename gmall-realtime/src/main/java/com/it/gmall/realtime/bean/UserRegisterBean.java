package com.it.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ZuYingFang
 * @time 2023-03-16 15:42
 * @description DwsUserUserRegisterWindow使用
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserRegisterBean {

    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 注册用户数
    Long registerCt;
    // 时间戳
    Long ts;


}
