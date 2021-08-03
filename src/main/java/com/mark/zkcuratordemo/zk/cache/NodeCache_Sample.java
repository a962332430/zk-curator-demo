package com.mark.zkcuratordemo.zk.cache;

import com.mark.zkcuratordemo.zk.constant.CommonConstant;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * @author chenguo 2021年07月30日
 * @version 1.0
 * @Description - TODO
 */
public class NodeCache_Sample {

    static String path = "/zk-book/nodecache/test";
    static CuratorFramework client = CuratorFrameworkFactory.builder().connectString(CommonConstant.CONN_STRING)
            .sessionTimeoutMs(60000)
            .connectionTimeoutMs(15000)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();

    public static void main(String[] args) throws Exception {
        client.start();
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path,"init".getBytes());

        // 对当前节点监听。当ZNode的内容发生变化（包括节点被删除）时,就会回调nodeChanged()方法
        final NodeCache cache = new NodeCache(client, path,false);
        cache.start(true);
        cache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                ChildData currentData = cache.getCurrentData();
                String data = currentData == null ? "" : new String(currentData.getData());
                System.out.println("=====> Node data update, new Data: "+data);
            }
        });

        client.setData().forPath(path,"i love you".getBytes());
        Thread.sleep(1000);

        client.setData().forPath(path,"i love you too".getBytes());
        Thread.sleep(1000);

        client.delete().deletingChildrenIfNeeded().forPath(path);
        Thread.sleep(10000);

        Thread.sleep(1000*60);

        cache.close();
        client.close();
    }

}
