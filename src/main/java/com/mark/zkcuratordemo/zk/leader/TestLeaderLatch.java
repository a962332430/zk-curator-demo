package com.mark.zkcuratordemo.zk.leader;

import com.chenguo.zk.constant.CommonConstant;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author chenguo 2021年07月30日
 * @version 1.0
 * @Description - Leader Latch。当多实例环境下，某资源（如分布式定时任务）只能同一时刻单实例跑时，可以通过zk竞选一个leader来保证（类似分布式锁）。非公平锁
 *
 * Leader Latch模式总结
 * 1. Leader Latch实现很简单，每一轮的选举算法都一样。
 * 2. 非公平模式，每一次选举都是随机,谁抢到就是谁的,假如是第二次选举,每个 Follower 通过 Watch 感知到节点被删除的时间不完全一样，只要有一个 Follower 得到通知即发起竞选。
 * 3. 给zookeeper造成的负载大,假如有上万个客户端都参与竞选,意味着同时会有上万个写请求发送给 Zookeeper。同时一旦 Leader 放弃领导权，Zookeeper 需要同时通知上万个 Follower，负载较大(羊群效应)
 */
public class TestLeaderLatch {

    private static final String PATH = "/demo/leader";
    /** 5个客户端 */
    private static final Integer CLIENT_COUNT = 5;

    public static void main(String[] args) throws Exception {
        // 5个线程模拟5个客户端，竞选leader
        ExecutorService service = Executors.newFixedThreadPool(CLIENT_COUNT);
        for (int i = 0; i < CLIENT_COUNT ; i++) {
            final int index = i;
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        new TestLeaderLatch().leaderSelect(index);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        // 休眠50秒之后结束main方法
        Thread.sleep(30 * 1000);
        service.shutdownNow();
    }

    // 每个客户端均竞选leader
    private void leaderSelect(int thread) throws Exception {

        //获取一个client
        CuratorFramework client = this.getClient(thread);
        //获取一个latch
        LeaderLatch latch = new LeaderLatch(client, PATH, String.valueOf(thread));

        //给latch添加监听，在
        latch.addListener(new LeaderLatchListener() {

            @Override
            public void notLeader() {
                //如果不是leader
                System.out.println("Client [" + thread + "] I am the follower !");
            }

            @Override
            public void isLeader() {
                //如果是leader
                System.out.println("Client [" + thread + "] I am the leader !");
            }
        });

        //开始选取 leader
        latch.start();

        //每个线程 休眠时间不一样,但是最大不能超过 main方法中的那个休眠时间,那个是50秒 到时候main方法结束 会中断休眠时间
        Thread.sleep(2 * (thread + 5) * 1000);
        if (latch != null) {
            //释放leadership
            //CloseMode.NOTIFY_LEADER 节点状态改变时,通知LeaderLatchListener
            latch.close(LeaderLatch.CloseMode.NOTIFY_LEADER);
        }
        if (client != null) {
            client.close();
        }
        System.out.println("Client [" + latch.getId() + "] Server closed...");
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
