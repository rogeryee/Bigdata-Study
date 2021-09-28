package com.yee.study.bigdata.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Curator 示例
 *
 * @author Roger.Yi
 */
public class CuratorSample {

    private static final Logger logger = LoggerFactory.getLogger(CuratorSample.class);

    /**
     * zookeeper地址
     */
    static final String CONNECT_ADDR = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    /**
     * session超时时间
     */
    static final int SESSION_OUTTIME = 5000;//ms

    public static void main(String[] args) throws Exception {

        //1 重试策略：初试时间为1s 重试10次
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);

        //2 通过工厂创建连接
        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .sessionTimeoutMs(SESSION_OUTTIME)
                .retryPolicy(retryPolicy)
                .build();

        //3 开启连接
        cf.start();

        logger.info("Zookeeper status=[{}]", cf.getState());

        //4 建立节点 指定节点类型（不加withMode默认为持久类型节点）、路径、数据内容
        cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/zk-test/c1", "c1内容".getBytes());

        //5. 读取节点
        String ret1 = new String(cf.getData().forPath("/zk-test/c1"));
        logger.info("get /zk-test/c1 = [{}]", ret1);

        //6. 修改节点
        cf.setData().forPath("/zk-test/c1", "修改c1内容".getBytes());
        String ret2 = new String(cf.getData().forPath("/zk-test/c1"));
        logger.info("set /zk-test/c1 = [{}]", ret2);

        //7 删除节点
        cf.delete().guaranteed().deletingChildrenIfNeeded().forPath("/zk-test");

        // 绑定回调函数
        ExecutorService pool = Executors.newCachedThreadPool();
        cf.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).inBackground(new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework cf, CuratorEvent ce) throws Exception {
                logger.info("create /zk-test-c3 ResultCode=[{}], type=[{}]", ce.getResultCode(), ce.getType());
            }
        }, pool).forPath("/zk-test-c3", "c3内容".getBytes());

        Thread.sleep(Integer.MAX_VALUE);
        cf.close();
    }
}
