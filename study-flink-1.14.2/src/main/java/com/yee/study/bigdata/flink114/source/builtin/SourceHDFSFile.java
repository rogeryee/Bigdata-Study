package com.yee.study.bigdata.flink114.source.builtin;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * 读取 HDFS 文件进行演示
 *
 * @author Roger.Yee
 */
public class SourceHDFSFile {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(2);

        // 读取 HDFS 文件，需要准备 core-site.xml 和 hdfs-site.xml 文件放置到项目中
        DataSource<String> lineDS = executionEnvironment.readTextFile("hdfs:///yish/test/test.txt");

        // 输出
        lineDS.print();
    }
}
