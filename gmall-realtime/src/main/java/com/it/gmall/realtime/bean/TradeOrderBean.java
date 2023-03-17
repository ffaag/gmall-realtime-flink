package com.it.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ZuYingFang
 * @time 2023-03-17 18:11
 * @description DwsTradeOrderWindow用
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TradeOrderBean {

    // 窗口起始时间
    String stt;

    // 窗口关闭时间
    String edt;

    // 下单独立用户数
    Long orderUniqueUserCount;

    // 下单新用户数
    Long orderNewUserCount;

    // 下单活动减免金额
    Double orderActivityReduceAmount;

    // 下单优惠券减免金额
    Double orderCouponReduceAmount;

    // 下单原始金额
    Double orderOriginalTotalAmount;

    // 时间戳
    Long ts;


}
