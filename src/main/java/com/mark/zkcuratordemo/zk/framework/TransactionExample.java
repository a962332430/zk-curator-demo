package com.mark.zkcuratordemo.zk.framework;

import com.mark.zkcuratordemo.zk.constant.CommonConstant;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Collection;

/**
 * @author chenguo 2021年07月30日
 * @version 1.0
 * @Description - TODO
 */
public class TransactionExample {

    public static void main(String[] args) throws Exception {

        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3,Integer.MAX_VALUE);
        CuratorFramework client = CuratorFrameworkFactory.newClient(CommonConstant.CONN_STRING, retryPolicy);
        client.start();

        client.delete().forPath("/a");

        //开始事务操作，原子操作
        CuratorOp createParentNode = client.transactionOp().create().forPath("/a", "some data".getBytes());
        CuratorOp createChildNode = client.transactionOp().create().forPath("/a/path", "other data".getBytes());
        CuratorOp setParentNode = client.transactionOp().setData().forPath("/a", "other data".getBytes());
        CuratorOp deleteParent = client.transactionOp().delete().forPath("/a/path");

        Collection<CuratorTransactionResult> results = client.transaction().forOperations(createParentNode, createChildNode, setParentNode,deleteParent);

        for ( CuratorTransactionResult result : results ){
            System.out.println(result.getForPath() + " - " + result.getType());
        }
    }
}
