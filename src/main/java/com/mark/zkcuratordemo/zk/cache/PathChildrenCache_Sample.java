package com.mark.zkcuratordemo.zk.cache;

import com.chenguo.zk.constant.CommonConstant;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * @author chenguo 2021年07月30日
 * @version 1.0
 * @Description - TODO
 */
public class PathChildrenCache_Sample {

    static String path = "/zk-book2";
    static CuratorFramework client = CuratorFrameworkFactory.builder().connectString(CommonConstant.CONN_STRING)
            .sessionTimeoutMs(60000)
            .connectionTimeoutMs(15000)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build();

    public static void main(String[] args) throws Exception {
        client.start();

        // 当指定节点的子节点发生变化时，就会回调该方法。
        // PathChildrenCacheListener类中定义了所有的事件类型，主要包括新增子节点(CHILD_ADDED)、子节点数据变更(CHILD_UPDATED)、和子节点的删除(CHILD_REMOVED)三类。
        PathChildrenCache cache = new PathChildrenCache(client,path,true);
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED:
                        System.out.println("=====> CHILD_ADDED : "+ event.getData().getPath() + "  数据:" + new String(event.getData().getData()));
                        break;
                    case CHILD_REMOVED:
                        System.out.println("=====> CHILD_REMOVED : "+ event.getData().getPath() + "  数据:" + new String(event.getData().getData()));
                        break;
                    case CHILD_UPDATED:
                        System.out.println("=====> CHILD_UPDATED : "+ event.getData().getPath() + "  数据:" + new String(event.getData().getData()));
                        break;
                    default:
                        break;
                }
            }
        });
        client.create().withMode(CreateMode.PERSISTENT).forPath(path,"init".getBytes());
        Thread.sleep(1000);

        // 新增子节点
        client.create().withMode(CreateMode.PERSISTENT).forPath(path+"/c1");
        Thread.sleep(1000);
        // 修改子节点内容
        client.setData().forPath(path+"/c1","I love you".getBytes());
        Thread.sleep(1000*60);
        // 删除子节点
        client.delete().forPath(path+"/c1");
        Thread.sleep(1000);
        // 删除当前节点
        client.delete().forPath(path);
        Thread.sleep(10000);

        cache.close();
        client.close();
    }
}
