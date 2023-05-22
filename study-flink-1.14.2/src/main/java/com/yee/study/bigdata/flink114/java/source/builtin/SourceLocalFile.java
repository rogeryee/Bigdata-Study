package com.yee.study.bigdata.flink114.java.source.builtin;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * 读取 HDFS 文件进行演示
 *
 * @author Roger.Yee
 */
public class SourceLocalFile {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        // 读取本地文件
        DataSource<String> lineDS = executionEnvironment.readTextFile("file:///Users/cntp/MyWork/yee/bigdata-study/study-flink-1.14.2/data/test.txt");

        // 输出
        lineDS.print();
    }
}
