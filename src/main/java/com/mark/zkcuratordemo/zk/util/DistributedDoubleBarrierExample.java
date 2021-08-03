package com.mark.zkcuratordemo.zk.util;

import com.mark.zkcuratordemo.zk.constant.CommonConstant;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author chenguo 2021年07月30日
 * @version 1.0
 * @Description - TODO
 */
public class DistributedDoubleBarrierExample {

    private static final String PATH = "/examples/barrier";

    /** 客户端数量 */
    private static final int CLIENT_COUNT = 5;

    public static void main(String[] args) throws Exception {

        for(int i=0;i<CLIENT_COUNT;i++){
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        CuratorFramework client = CuratorFrameworkFactory.newClient(CommonConstant.CONN_STRING,3000,3000, new ExponentialBackoffRetry(1000, 3,Integer.MAX_VALUE));
                        client.start();

                        //获取DistributedDoubleBarrier
                        DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, PATH,CLIENT_COUNT);
                        System.out.println("线程" +index+" 等待");

                        barrier.enter();
                        //调用enter阻塞,直到所有线程都到达之后执行
                        System.out.println("线程" +index+" 已执行");

                        // 执行完毕之后调用leave阻塞,直到所有线程都调用leave
                        barrier.leave();
                        System.out.println("flag!");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

        Thread.sleep(Integer.MAX_VALUE);
    }
}
