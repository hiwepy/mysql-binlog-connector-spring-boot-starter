package com.alibaba.otter.canal.annotation;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.lang.annotation.*;

/**
 * 监听数据库的操作
 *
 * @author lujun
 */

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OnCanalEvent {

    /**
     * canal 指令
     * default for all
     * @return destination name
     */
    String destination() default "";

    /**
     * 数据库实例
     * default for all
     * @return schema name
     */
    String schema() default "*";

    /**
     * 监听的表
     * default for all
     * @return table name
     */
    String table() default "*";

    /**
     * 监听操作的类型
     * default for all\
     * @return CanalEntry.EventType
     */
    CanalEntry.EventType[] eventType();

}
