package com.mark.zkcuratordemo.zk.framework;

import com.mark.zkcuratordemo.zk.constant.CommonConstant;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

/**
 * @author chenguo 2021年07月30日
 * @version 1.0
 * @Description - TODO
 */
public class CrudExample {

    public static void main(String[] args) throws Exception {
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3,Integer.MAX_VALUE);
        CuratorFramework client = CuratorFrameworkFactory.newClient(CommonConstant.CONN_STRING, retryPolicy);
        client.start();

//        testCreateNode(client);

//        testQueryNode(client);

//        testUpdateNode(client);

        testDeleteDode(client);
    }

    private static void testDeleteDode(CuratorFramework client) throws Exception {
        Stat existsNodeStat = client.checkExists().forPath("/tom/client3");
        if(existsNodeStat == null){
            System.out.println("=====>节点不存在");
        }
        if(existsNodeStat.getEphemeralOwner() > 0){
            System.out.println("=====>临时节点");
        }else{
            System.out.println("=====>持久节点");
        }
        client.delete()
                .guaranteed()
                .forPath("/tom/client3");
    }

    private static void testUpdateNode(CuratorFramework client) throws Exception {
        Stat stat = client.setData()
                .withVersion(-1)
                .forPath("/tom/client3", "I love you".getBytes());
        System.out.println("=====>修改之后的版本为：" + stat.getVersion());
    }

    private static void testQueryNode(CuratorFramework client) throws Exception {
        Stat node10Stat = new Stat();
        byte[] bytes = client.getData()
                .storingStatIn(node10Stat) //获取stat信息存储到stat对象
                .usingWatcher(new Watcher() { // 该Watcher只能触发一次
                    @Override
                    public void process(WatchedEvent event) {
                        System.out.println("=====>watcher触发了。。。。");
                        System.out.println(event);
                    }})
                .forPath("/tom/client3");
        System.out.println("=====>该节点的数据版本号为：" + node10Stat.getVersion());
        System.out.println("=====>获取到的节点数据为："+new String(bytes));
    }

    private static void testCreateNode(CuratorFramework client) throws Exception {
        // PERSISTENT_SEQUENTIAL_WITH_TTL Curator 5.0后续增加永久定时节点（在一定的时间内,如果没有操作该节点,并且该节点没有子节点,那么服务器将自动删除该节点）
        client.create()
                .creatingParentContainersIfNeeded() // 递归创建，若没有父节点则自动创建
                .withMode(CreateMode.PERSISTENT)  // 节点类型。共四类：永久，临时，永久顺序，临时顺序。
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE) // 设置ACL鉴权
                .forPath("/test/30", "123456".getBytes());
    }
}
