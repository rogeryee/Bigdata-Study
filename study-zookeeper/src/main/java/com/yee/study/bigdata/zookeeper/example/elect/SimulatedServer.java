package com.yee.study.zookeeper.example.elect;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 模拟服务器（Zookeeper模拟Master选举的）
 *
 * @author Roger.Yi
 */
public class SimulatedServer {

    private static final Logger logger = LoggerFactory.getLogger(SimulatedServer.class);

    /**
     * 服务器名称
     */
    private String name;

    /**
     * 连接配置
     */
    private String connectStr;

    /**
     * 监听路径
     */
    private String zkPath;

    /**
     * curator client
     */
    private CuratorFramework client;

    /**
     * 监听器
     */
    private TreeCache cache;

    public SimulatedServer(String name, String connectStr, String path) {
        this.name = name;
        this.connectStr = connectStr;
        this.zkPath = path;
    }

    /**
     * 启动
     *
     * @throws IOException
     */
    public void start() throws IOException {
        client = CuratorFrameworkFactory.builder()
                .connectString(connectStr)
                .connectionTimeoutMs(1000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        client.start();

        try {
            // 尝试创建监听节点
            createZKNode(zkPath);
        } catch (Exception e) {
            logger.info("Server [{}] : create node failed, waiting for master election.", name);
            try {
                addZKNodeListener(zkPath);
            } catch (Exception ex) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建节点
     *
     * @param path
     * @throws Exception
     */
    private void createZKNode(String path) throws Exception {
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
        logger.info("Server [{}] : create node successfully, become the MASTER.", name);
    }

    /**
     * 添加监听器
     * @param path
     * @throws Exception
     */
    private void addZKNodeListener(String path) throws Exception {
        cache = new TreeCache(client, path);
        cache.start();
        cache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                if (event.getData() != null && event.getType() == TreeCacheEvent.Type.NODE_REMOVED) {
                    logger.info("Server [{}] : Master node lost, start the MASTER elect.", name);
                    createZKNode(path);
                }
            }
        });
    }

}
