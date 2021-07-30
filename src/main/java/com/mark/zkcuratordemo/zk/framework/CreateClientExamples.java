package com.mark.zkcuratordemo.zk.framework;

import com.chenguo.zk.constant.CommonConstant;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author chenguo 2021年07月30日
 * @version 1.0
 * @Description - TODO
 */
public class CreateClientExamples {

    public static void main(String[] args) throws Exception {

        // 重连时间间隔，以及重试次数
        // 随着重试次数增加重试时间间隔变大,指数倍增长baseSleepTimeMs * Math.max(1, random.nextInt(1 << (retryCount + 1)))
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3,Integer.MAX_VALUE);


        /**
         * connectionString zk地址
         * retryPolicy 重试策略
         * 默认的sessionTimeoutMs为60000
         * 默认的connectionTimeoutMs为15000
         */
        CuratorFramework client1 = CuratorFrameworkFactory.newClient(CommonConstant.CONN_STRING,retryPolicy);
        client1.start();
        client1.create().forPath("/test/client1", "1".getBytes());


        /**
         * connectionString zk地址
         * retryPolicy 重试策略
         * 自定义sessionTimeoutMs为60000
         * 自定义connectionTimeoutMs为15000
         */
        CuratorFramework client2 = CuratorFrameworkFactory.
                newClient(CommonConstant.CONN_STRING, 1000*60, 1000*15, retryPolicy);
        client2.start();
        client2.create().forPath("/test/client2", "2".getBytes());

        /**
         * connectionString zk地址
         * retryPolicy 重试策略
         * 自定义sessionTimeoutMs为60000
         * 自定义connectionTimeoutMs为15000
         * namespace为独立的命名空间,之后操作都是基于该命名空间。表明该client下的所有节点操作都存在父目录 /tom/xxx
         */
        CuratorFramework client3 = CuratorFrameworkFactory.builder()
                .connectString(CommonConstant.CONN_STRING)
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(1000*60)
                .connectionTimeoutMs(1000*15)
                .namespace("tom")
                .build();
        client3.start();
        client3.create().forPath("/client3", "3".getBytes());


    }
}
