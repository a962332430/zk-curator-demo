package com.mark.zkcuratordemo.zk.util;

import com.chenguo.zk.constant.CommonConstant;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author chenguo 2021年07月30日
 * @version 1.0
 * @Description - 首先尝试使用乐观锁的方式设置计数器， 如果不成功(比如期间计数器已经被其它client更新了)， 它使用InterProcessMutex(可重入锁)方式来更新计数值
 * 必须检查返回结果的succeeded()， 它代表此操作是否成功。 如果操作成功， preValue()代表操作前的值， postValue()代表操作后的值。
 */
public class DistributedAtomicLongExample {

    private static final int CLIENT_COUNT = 5;
    private static final String PATH = "/examples/counter";
    public static void main(String[] args) throws Exception {
        try{
            CuratorFramework client = CuratorFrameworkFactory.newClient(CommonConstant.CONN_STRING,3000,3000, new ExponentialBackoffRetry(1000, 3,Integer.MAX_VALUE));
            client.start();
            List<DistributedAtomicLong> examples = new ArrayList<>();
            ExecutorService service = Executors.newFixedThreadPool(CLIENT_COUNT);
            for (int i = 0; i < CLIENT_COUNT; ++i) {
                final DistributedAtomicLong count = new DistributedAtomicLong(client, PATH, new RetryNTimes(10, 10));

                examples.add(count);
                Callable<Void> task = new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        try {
                            AtomicValue<Long> value = count.increment();
                            System.out.println("操作是否成功: " + value.succeeded());
                            if (value.succeeded()){
                                System.out.println("操作成功: 操作前的值为： " + value.preValue() + " 操作后的值为： " + value.postValue());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return null;
                    }
                };
                service.submit(task);
            }
            service.shutdown();
            service.awaitTermination(10, TimeUnit.MINUTES);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
