package com.yee.study.bigdata.flink.window;

import com.yee.study.bigdata.flink.window.support.WordSplitFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

/**
 * 自定义触发器 示例
 * <p>
 * 需求：使用 Trigger 自己实现一个类似 CountWindow 的效果
 * 具体实现： countWindow(3)
 * 1、这个单词出现了 3 次就输出一次。wordcount 需求下，能看到这样的效果
 * 2、这个 key 的 window 内部有三个元素了，就触发计算，做输出
 *
 * @author Roger.Yi
 */
public class TriggerSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source
        DataStreamSource<String> source = env.socketTextStream("localhost", 6789);

        // Operator
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds = source
                .flatMap(new WordSplitFunction())
                .keyBy(tuple -> tuple.f0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(3))
                .sum(1);

        // Sink
        ds.print().setParallelism(1);

        // run
        env.execute("TimeWindowSample");
    }
}
