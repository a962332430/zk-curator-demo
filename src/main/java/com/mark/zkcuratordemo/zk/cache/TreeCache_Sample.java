package com.mark.zkcuratordemo.zk.cache;

import com.mark.zkcuratordemo.zk.constant.CommonConstant;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * @author chenguo 2021年07月30日
 * @version 1.0
 * @Description - TODO
 */
public class TreeCache_Sample {

    static String path = "/zk-book3";
    static CuratorFramework client = CuratorFrameworkFactory.builder().connectString(CommonConstant.CONN_STRING)
            .sessionTimeoutMs(60000)
            .connectionTimeoutMs(15000)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();

    public static void main(String[] args) throws Exception {
        client.start();
        TreeCache cache = new TreeCache(client,path);

        cache.start();

        //添加错误监听器
        cache.getUnhandledErrorListenable().addListener(new UnhandledErrorListener() {
            @Override
            public void unhandledError(String s, Throwable throwable) {
                System.out.println("======>  错误原因：" + throwable.getMessage());
            }
        });

        //节点变化的监听器
        cache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                switch (treeCacheEvent.getType()) {
                    case INITIALIZED:
                        System.out.println("=====> INITIALIZED :  初始化");
                        break;
                    case NODE_ADDED:
                        System.out.println("=====> NODE_ADDED : "+ treeCacheEvent.getData().getPath() +"  数据:"+ new String(treeCacheEvent.getData().getData()));
                        break;
                    case NODE_REMOVED:
                        System.out.println("=====> NODE_REMOVED : "+ treeCacheEvent.getData().getPath() +"  数据:"+ new String(treeCacheEvent.getData().getData()));
                        if("/zk-book3".equals(treeCacheEvent.getData().getPath())){
                            throw new RuntimeException("=====> 测试异常监听UnhandledErrorListener");
                        }
                        break;
                    case NODE_UPDATED:
                        System.out.println("=====> NODE_UPDATED : "+ treeCacheEvent.getData().getPath() +"  数据:"+ new String(treeCacheEvent.getData().getData()));
                        break;
                    default:
                        System.out.println("=====> treeCache Type:" + treeCacheEvent.getType());
                        break;
                }
            }
        });
        client.create().withMode(CreateMode.PERSISTENT).forPath(path,"init".getBytes());
        Thread.sleep(3000);
        client.create().withMode(CreateMode.PERSISTENT).forPath(path+"/c1");
        Thread.sleep(3000);
        client.setData().forPath(path+"/c1","I love you".getBytes());
        Thread.sleep(3000);
        client.delete().forPath(path+"/c1");
        Thread.sleep(3000);
        client.delete().forPath(path);
        Thread.sleep(10000);
        cache.close();
        client.close();
    }
}
