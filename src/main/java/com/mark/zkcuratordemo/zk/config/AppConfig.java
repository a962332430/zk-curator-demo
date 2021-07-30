package com.mark.zkcuratordemo.zk.config;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author chenguo 2021年07月27日
 * @version 1.0
 * @Description - TODO
 */
@Configuration
public class AppConfig {

    @Bean
    public CuratorFramework client() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("10.172.16.13:2181")
                .sessionTimeoutMs(1000*10*60)
                .connectionTimeoutMs(1000*10*60)
                .retryPolicy(retryPolicy)
                .build();
        client.start();
        return client;
    }
}
