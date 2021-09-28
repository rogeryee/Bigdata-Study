package com.yee.study.bigdata.zookeeper.example.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;

/**
 * 采用Zookeeper分布式锁生成ID的示例
 *
 * 如果不采用分布式锁，那么在高并发的场景下即使采用了时间戳也无法避免ID重复
 *
 * @author Roger.Yi
 */
public class DistributedIdGenDemo implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DistributedIdGenDemo.class);

    /**
     * ID生成器
     */
    private static IdGenerator idGenerator = new IdGenerator();

    /**
     * 模拟同时并发的线程数
     */
    private static final int NUM = 10;

    /**
     * 按照线程数初始化倒计数器,倒计数器
     */
    private static CountDownLatch cdl = new CountDownLatch(NUM);

    /**
     * 基于Zookeeper的分布式锁
     */
    private Lock lock = new ImprovedDsLock(); // ImprovedDsLock    DistributedLock

    // 创建订单接口
    public void genId() {
        String id = null;
        lock.lock();
        
        try {
            // 获取订单编号
            id = idGenerator.genId();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        logger.info("Generator ID=[{}]", id);
    }

    @Override
    public void run() {
        try {
            // 等待其他线程初始化
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 生成ID
        genId();
    }

    public static void main(String[] args) {
        for (int i = 1; i <= NUM; i++) {
            new Thread(new DistributedIdGenDemo()).start();
            cdl.countDown();
        }
    }
}
