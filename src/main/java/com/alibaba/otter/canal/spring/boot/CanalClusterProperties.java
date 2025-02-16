package com.alibaba.otter.canal.spring.boot;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(CanalClusterProperties.PREFIX)
@Getter
@Setter
@ToString
public class CanalClusterProperties {

    public static final int DEFAULT_PORT = 11111;
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_MAX_SLEEP_MS = Integer.MAX_VALUE;
    public static final String PREFIX = "canal.cluster";

    /**
     * 配置信息
     */
    private List<CanalClusterProperties.Instance> instances = new ArrayList<>();

    @Data
    public static class Instance {

        /**
         * Canal Server 地址
         */
        private String addresses;
        /**
         * Canal Zookeeper 地址。如果设置了该属性，则忽略addresses属性。
         */
        private String zkServers;
        /**
         * Canal Destination 地址
         */
        private String destination;
        /**
         * Canal Server 账号
         */
        private String username;
        /**
         * Canal Server 密码
         */
        private String password;
        /**
         * Socket 连接超时时间，单位：毫秒。默认为 60000
         */
        private int soTimeout     = 60000;
        /**
         * Socket 空闲超时时间，单位：毫秒。默认为 3600000
         */
        private int idleTimeout   = 60 * 60 * 1000;
        /**
         * 重试次数;设置-1时可以subscribe阻塞等待时优雅停机
         */
        private int retryTimes    = 3;
        /**
         * 重试的时间间隔，默认5秒
         */
        private int retryInterval = 5000;

    }

}
