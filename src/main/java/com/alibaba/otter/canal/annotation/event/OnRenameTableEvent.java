package com.alibaba.otter.canal.annotation.event;

import com.alibaba.otter.canal.annotation.OnCanalEvent;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * 重命名表
 *
 * @author lujun
 */

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@OnCanalEvent(eventType = CanalEntry.EventType.RENAME)
public @interface OnRenameTableEvent {

    /**
     * canal 指令
     * default for all
     * @return canal 指令
     */
    @AliasFor(annotation = OnCanalEvent.class)
    String destination() default "";

    /**
     * 数据库实例
     * @return 数据库实例
     */
    @AliasFor(annotation = OnCanalEvent.class)
    String schema();
}
