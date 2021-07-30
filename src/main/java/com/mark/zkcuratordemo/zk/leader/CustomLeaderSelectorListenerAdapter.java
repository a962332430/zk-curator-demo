package com.mark.zkcuratordemo.zk.leader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chenguo 2021年07月30日
 * @version 1.0
 * @Description - 实现LeaderSelectorListener 或者 继承LeaderSelectorListenerAdapter，用于定义获取领导权后的业务逻辑
 */
public class CustomLeaderSelectorListenerAdapter extends LeaderSelectorListenerAdapter implements Closeable {

    /** 客户端名称 */
    private String name;
    /** leaderSelector */
    private LeaderSelector leaderSelector;
    /** 原子性的 用来记录获取 leader的次数 */
    public AtomicInteger leaderCount = new AtomicInteger(1);

    public CustomLeaderSelectorListenerAdapter(CuratorFramework client,String path,String name ){
        this.name = name;
        this.leaderSelector = new LeaderSelector(client, path, this);

        /**
         * 自动重新排队
         * 该方法的调用可以确保此实例在释放领导权后还可能获得领导权
         */
        leaderSelector.autoRequeue();
    }

    /**
     * 启动  调用leaderSelector.start()
     * @throws IOException
     */
    public void start() throws IOException {
        leaderSelector.start();
    }

    /**
     * 获取领导权之后执行的业务逻辑，执行完自动放弃领导权
     * @param client
     * @throws Exception
     */
    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        final int waitSeconds = 2;
        System.out.println(name + "成为当前leader" + " 共成为leader的次数：" + leaderCount.getAndIncrement() + "次");
        try{
            //模拟业务逻辑执行2秒
            Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
        }catch ( InterruptedException e ){
            System.err.println(name + "已被中断");
            Thread.currentThread().interrupt();
        }finally{
            System.out.println(name + "放弃领导权");
        }
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }
}
