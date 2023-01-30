package com.yee.study.bigdata.flink.sink.builtin;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 输出到文件
 *
 * @author Roger.Yi
 */
public class SinkWrite {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Source
        List<String> data = new ArrayList<>();
        data.add("huangbo");
        data.add("xuzheng");
        data.add("wangbaoqiang");
        data.add("shenteng");

        DataStreamSource<String> ds = env.fromCollection(data);

        // Output
        ds.writeAsText("file:///Users/cntp/test.txt").setParallelism(1);

        env.execute("SinkWrite");
    }
}
