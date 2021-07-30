package com.mark.zkcuratordemo.zk.async;

import com.chenguo.zk.constant.CommonConstant;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author chenguo 2021年07月30日
 * @version 1.0
 * @Description - TODO
 */
public class AsyncExample {

    /**
     * inBackground()方法就是Curator提供的异步调用入口。对应的异步处理接口为BackgroundCallback
     * 此接口指提供了一个processResult()的方法，用来处理回调结果
     */
    public static void main(String[] args) throws Exception {

        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3,Integer.MAX_VALUE);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(CommonConstant.CONN_STRING, retryPolicy);
        curatorFramework.start();

        //=========================创建节点=============================

        // executor参数：此接口还允许传入一个Executor实例，用一个专门线程池来处理返回结果之后的业务逻辑。
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        curatorFramework.create()
                .creatingParentsIfNeeded()//递归创建,如果没有父节点,自动创建父节点
                .withMode(CreateMode.PERSISTENT)//节点类型,持久节点
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)//设置ACL,和原生API相同
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        System.out.println("===>响应码：" + event.getResultCode()+",type:" + event.getType());
                        System.out.println("===>Thread of processResult:"+Thread.currentThread().getName());
                        System.out.println("===>context参数回传：" + event.getContext());
                    }
                },"传给服务端的内容,异步会传回来", executorService)
                .forPath("/node10","123456".getBytes());
        Thread.sleep(3000);


        curatorFramework.create()
                .creatingParentsIfNeeded()//递归创建,如果没有父节点,自动创建父节点
                .withMode(CreateMode.PERSISTENT)//节点类型,持久节点
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)//设置ACL,和原生API相同
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        System.out.println("===>响应码：" + event.getResultCode()+",type:" + event.getType());
                        System.out.println("===>Thread of processResult:"+Thread.currentThread().getName());
                        System.out.println("===>context参数回传：" + event.getContext());
                    }
                },"传给服务端的内容,异步会传回来")
                .forPath("/node10","123456".getBytes());
        Thread.sleep(3000);

        executorService.shutdown();
    }

}
