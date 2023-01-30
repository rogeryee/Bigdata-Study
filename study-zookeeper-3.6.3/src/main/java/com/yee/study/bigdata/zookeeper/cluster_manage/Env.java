package com.yee.study.bigdata.zookeeper.cluster_manage;

/**
 * 环境变量
 *
 * @author Roger.Yi
 */
public final class Env {

    // 请求zookeeper连接的连接信息（主机名 + 端口号）
    public static final String ConnectStr = "10.100.130.11:2191,10.100.130.11:2192,10.100.130.11:2193";

    // 请求连接的超时时长（单位：毫秒）
    public static final int TimeOut = 5000;

    // namenode监控的服务器列表的根节点
    public static final String ParentNode = "/servers";

    // datanode上线之后存储在zookeeper文件系统上的数据格式
    public static final String ChildNode = ParentNode + "/childNode";
}
