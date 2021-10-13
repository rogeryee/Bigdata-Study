package com.yee.study.bigdata.zookeeper.hw;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Roger.Yi
 */
public class WatcherTest {

    private static final Logger logger = LoggerFactory.getLogger(WatcherTest.class);

    private static ZooKeeper zk = null;
    private static final String CONNECT_STRING = "10.100.130.11:2191,10.100.130.11:2192,10.100.130.11:2193";
    private static final int Session_TimeOut = 4000;

    private static final String TEST_PATH = "/test";

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        zk = new ZooKeeper(CONNECT_STRING, Session_TimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                logger.info("watchedEvent=" + watchedEvent);
            }
        });

        zk.getData(TEST_PATH, true, null);

        Thread.sleep(Long.MAX_VALUE);
    }
}
