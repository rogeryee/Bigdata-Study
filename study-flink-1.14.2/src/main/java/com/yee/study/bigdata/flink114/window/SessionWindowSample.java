package com.yee.study.bigdata.flink114.window;

import com.yee.study.bigdata.flink114.window.support.WordSplitFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * SessionWindow 示例
 * <p>
 * 需求： 5秒 过去以后，该单词不出现就打印出来该单词和它的出现次数
 *
 * @author Roger.Yi
 */
public class SessionWindowSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source
        DataStreamSource<String> source = env.socketTextStream("localhost", 6789);

        // Operator
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds = source
                .flatMap(new WordSplitFunction())
                .keyBy(tuple -> tuple.f0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
                .sum(1);

        // Sink
        ds.print().setParallelism(1);

        // run
        env.execute("SessionWindowSample");
    }
}
