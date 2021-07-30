package com.mark.zkcuratordemo.zk.leader;

import com.chenguo.zk.constant.CommonConstant;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author chenguo 2021年07月30日
 * @version 1.0
 * @Description - LeaderElection 当多实例环境下，某资源（如分布式定时任务）只能同一时刻单实例跑时，可以通过zk竞选一个leader来保证（类似分布式锁）。公平锁
 *
 * LeaderElection模式总结
 * 1.扩展性好，每个客户端都只Watch 一个节点且每次节点被删除只须通知一个客户端
 * 2.旧 Leader 放弃领导权时，其它客户端根据竞选的先后顺序（也即节点序号）成为新 Leader，这也是公平模式的由来。
 * 3.延迟相对非公平模式要高，因为它必须等待特定节点得到通知才能选出新的 Leader。
 */
public class TestLeaderElection {

    private static final String PATH = "/demo/leader";
    /** 3个客户端 */
    private static final Integer CLIENT_COUNT = 5;

    public static void main(String[] args) throws Exception {
        // 5个线程模拟5个客户端，竞选leader
        ExecutorService service = Executors.newFixedThreadPool(CLIENT_COUNT);
        for (int i = 0; i < CLIENT_COUNT; i++) {
            final int index = i;
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        new TestLeaderElection().leaderSelect(index);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        Thread.sleep(30 * 1000);
        service.shutdownNow();
    }

    private void leaderSelect(final int thread) throws Exception {
        CuratorFramework client = this.getClient(thread);
        CustomLeaderSelectorListenerAdapter leaderSelectorListener =
                new CustomLeaderSelectorListenerAdapter(client, PATH, "Client #" + thread);
        leaderSelectorListener.start();
    }

    private CuratorFramework getClient(final int thread) {
        RetryPolicy rp = new ExponentialBackoffRetry(1000, 3);
        // Fluent风格创建
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(CommonConstant.CONN_STRING)
                .sessionTimeoutMs(1000000)
                .connectionTimeoutMs(3000)
                .retryPolicy(rp)
                .build();
        client.start();
        System.out.println("Client [" + thread + "] Server connected...");
        return client;
    }

}
