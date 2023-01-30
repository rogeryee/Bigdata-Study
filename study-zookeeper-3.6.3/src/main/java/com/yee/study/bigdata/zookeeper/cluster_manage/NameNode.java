package com.yee.study.bigdata.zookeeper.cluster_manage;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Roger.Yi
 */
public class NameNode {

    private static final Logger logger = LoggerFactory.getLogger(NameNode.class);

    private static List<String> dataNodeList = new ArrayList<>();

    private static ZooKeeper zk;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        zk = new ZooKeeper(Env.ConnectStr, Env.TimeOut, event -> {
            String path = event.getPath();
            Watcher.Event.EventType type = event.getType();
            logger.info(path + " - " + type);

            if(Env.ParentNode.equals(path) == false) {
                return;
            }

            try {
                List<String> currentNodeList = zk.getChildren(Env.ParentNode, false);
                List<String> diffList = currentNodeList.stream().filter(x -> !dataNodeList.contains(x)).collect(Collectors.toList());
                diffList.forEach(dataNode -> {
                    logger.info(dataNode + "changed.");
                });

                dataNodeList = currentNodeList;
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                zk.getChildren(Env.ParentNode, true);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        if(zk.exists(Env.ParentNode, false) == null) {
            zk.create(Env.ParentNode, "NameNode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        zk.getChildren(Env.ParentNode, true);
        logger.info("NameNode online....");

        Thread.sleep(Integer.MAX_VALUE);

        logger.info("NameNode offline....");
        zk.close();
    }
}
