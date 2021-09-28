package com.yee.study.bigdata.zookeeper.zkapi;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.apache.zookeeper.Watcher.Event.EventType;
import static org.apache.zookeeper.Watcher.Event.KeeperState;
import static org.apache.zookeeper.ZooDefs.Ids;

/**
 * 使用原生Zookeeper API 示例
 *
 * @author Roger.Yi
 */
public class ZkApiSample implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ZkApiSample.class);

    private static final CountDownLatch cdl = new CountDownLatch(1);

    private static ZooKeeper zk = null;

    private static Stat stat = new Stat();

    public static void main(String[] args) throws Exception {
        zk = new ZooKeeper("127.0.0.1:2181", 5000, new ZkApiSample());
        cdl.await();

        String rootPath = "/zk-test";

        // 创建节点示例
        doCreateData(rootPath);

        // 修改节点示例
//        doDataChange(rootPath);

        // 操作子节点
//        doChildNodeData(rootPath);

        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 创建节点示例
     *
     * @throws Exception
     */
    private static void doCreateData(String path) throws Exception {

        // 创建临时节点
        String path1 = zk.create(path, "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        logger.info("create ephemeral path=[{}] end.", path1);

        // 创建临时顺序节点
        String path2 = zk.create(path, "456".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.info("create ephemeral sequential path=[{}] end.", path2);

        // 创建临时顺序节点
        zk.create(path, "789".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                logger.info("process create ephemeral sequential result : " + rc + ", path = " + path + ", ctx = " + ctx + ", name = " + name);
            }
        }, "I am context");
    }

    /**
     * 修改数据
     *
     * @throws Exception
     */
    private static void doDataChange(String path) throws Exception {

        String path1 = zk.create(path, "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zk.getData(path, true, stat);
        logger.info("get path=[{}] : czxid=[{}], mzxid=[{}], version=[{}]", path, stat.getCzxid(), stat.getMzxid(), stat
                .getVersion());

        zk.setData(path, "123456".getBytes(), -1);

        zk.getData(path, true, stat);
        logger.info("get path=[{}] : czxid=[{}], mzxid=[{}], version=[{}]", path, stat.getCzxid(), stat.getMzxid(), stat
                .getVersion());
    }

    /**
     * 操作子节点
     *
     * @throws Exception
     */
    private static void doChildNodeData(String path) throws Exception {

        // 创建父节点
        zk.create(path, "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        int size = 3;
        for (int i = 1; i <= size; i++) {
            String childPath = path + "/" + "c" + i;
            logger.info("create child path=[{}]", childPath);
            zk.create(childPath, String.valueOf(i).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }

        List<String> list = zk.getChildren(path, true);
        for (String str : list) {
            logger.info("get child value=[{}]", str);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("Receive watched event: state=[{}] type=[{}]", event.getState(), event.getType());

        try {
            if (event.getState() == KeeperState.SyncConnected) {
                if (event.getType() == EventType.None && null == event.getPath()) {
                    cdl.countDown();
                } else if (event.getType() == EventType.NodeDataChanged) {
                    zk.getData(event.getPath(), true, stat);
                    logger.info("set path=[{}] end : czxid=[{}], mzxid=[{}], version=[{}]", event.getPath(), stat.getCzxid(), stat
                            .getMzxid(), stat.getVersion());
                } else if (event.getType() == EventType.NodeChildrenChanged) {
                    zk.getChildren(event.getPath(), true, stat);
                    logger.info("get child path=[{}] end : czxid=[{}], mzxid=[{}], version=[{}]", event.getPath(), stat.getCzxid(), stat
                            .getMzxid(), stat.getVersion());
                }
            }
        } catch (Exception e) {
            logger.error("process event meet error.", e);
        }
    }
}
