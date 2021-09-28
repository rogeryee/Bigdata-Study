package com.yee.study.zookeeper.example.elect;

/**
 * Master-Slave 示例（使用Zookeeper来协调Master选举）
 * @author Roger.Yi
 */
public class MasterSlaveDemo {

    /**
     * 分别启动2或多个MasterSlaveDemo实例：
     * 1. 可以发现其中只有一台服务器成为了Master；
     * 2. 如果将Master服务器停止，可以发现剩余的服务器会有一台成为Master
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String connectStr = "localhost:2181";
        String path = "/zk-elect";

        String serverName = args[0];
        SimulatedServer server1 = new SimulatedServer(serverName, connectStr, path);
        server1.start();

        Thread.sleep(Integer.MAX_VALUE);  
    }
}
