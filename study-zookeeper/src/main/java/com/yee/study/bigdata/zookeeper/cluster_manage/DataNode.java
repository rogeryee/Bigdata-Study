package com.yee.study.bigdata.zookeeper.cluster_manage;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 数据节点
 *
 * @author Roger.Yi
 */
public class DataNode {

    private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

    private static int index = 101;

    private static ZooKeeper zk;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(Env.ConnectStr, Env.TimeOut, null);

        String serverName = "DataNode-" + index;
        zk.create(Env.ChildNode + index, serverName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        logger.info(serverName + " online....");

        Thread.sleep(Integer.MAX_VALUE);

        logger.info(serverName + " offline....");
        zk.close();
    }
}
