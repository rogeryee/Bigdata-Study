package com.yee.study.bigdata.zookeeper.zkclient;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 使用ZkClient操作ZooKeeper的示例
 *
 * @author Roger.Yi
 */
public class ZkClientSample {

    public static final Logger logger = LoggerFactory.getLogger(ZkClientSample.class);

    private static ZkClient zkClient = null;

    public static void main(String[] args) throws Exception {
        zkClient = new ZkClient("127.0.0.1:2181", 5000, 5000, new SerializableSerializer());

        String path = "/zkclient-test";

        // 创建节点
//        doCreateData(path);

        // 操作节点（读/写）
//        doSetAndGet(path);

        // 操作子节点
        doChildNodeData(path);

        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 创建节点
     *
     * @throws Exception
     */
    private static void doCreateData(String path) throws Exception {
        User user = new User("Roger");
        String ret = zkClient.create(path, user, CreateMode.EPHEMERAL);
        logger.info("create path=[{}]", ret);
    }

    /**
     * 操作节点（读/写）
     *
     * @throws Exception
     */
    private static void doSetAndGet(String path) throws Exception {
        User user = new User("Roger");
        String ret = zkClient.create(path, user, CreateMode.EPHEMERAL);
        logger.info("create path=[{}]", ret);
        
        Stat stat = new Stat();
        User userAfterRead = zkClient.readData(ret, stat);
        logger.info("after set and read, user.name=[{}] stat=[{}]", userAfterRead.getName(), stat);
    }

    /**
     * 操作子节点
     *
     * @throws Exception
     */
    private static void doChildNodeData(String path) throws Exception {
        zkClient.create(path, "123".getBytes(), CreateMode.PERSISTENT);

        int size = 3;
        for (int i = 1; i <= size; i++) {
            String childPath = path + "/" + "c" + i;
            logger.info("create child path=[{}]", childPath);
            zkClient.create(childPath, String.valueOf(i).getBytes(), CreateMode.EPHEMERAL);
        }

        List<String> list = zkClient.getChildren(path);
        for (String str : list) {
            logger.info("get child value=[{}]", str);
        }
    }
}
