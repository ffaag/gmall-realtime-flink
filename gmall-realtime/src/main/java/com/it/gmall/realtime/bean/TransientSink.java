package com.it.gmall.realtime.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author ZuYingFang
 * @time 2023-03-14 15:14
 * @description 自定义注解
 */
@Target(ElementType.FIELD)    // 作用范围，只对列作用
@Retention(RetentionPolicy.RUNTIME)   // 生效时期，运行时生效
public @interface TransientSink {
}