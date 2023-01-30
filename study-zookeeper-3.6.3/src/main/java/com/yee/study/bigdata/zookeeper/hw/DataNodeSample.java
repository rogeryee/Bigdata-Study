package com.yee.study.bigdata.zookeeper.hw;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * ZKDataBase 示例
 *
 * @author Roger.Yi
 */
public class DataNodeSample {
}

@Data
class DataNode {
    private DataNode parent;

    private List<DataNode> children;

    private Byte[] value;
}

@Data
class DataTree {
    private DataNode root;
    private List<DataNode> allNodes;
    private Map<String, DataNode> path2nodeMap;

    public void insertNode(DataNode node) {
    }

    public void updateNode(DataNode node) {
    }

    public void deleteNode(String path) {
    }

    public void getNodeData(String path) {
    }

    public List<DataNode> getNodeChildren(String path, boolean recu) {
        return null;
    }
}

@Data
class ZKDatabase {
    private DataTree dt; // 数据存储模型抽象
    private FileTxnSnapLog fileTxnSnapLog; // 负责快照和日志功能的组件

    public void save(DataTree tree) {
    }

    public void restore(DataTree tree) {
    }
}

@Data
class FileTxnSnapLog {
    private TxnLog txnLog;
    private Snapshot sh;
}

class TxnLog {

}

class Snapshot {

}