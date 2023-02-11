package com.yee.study.bigdata.flink114.java.window;

import com.yee.study.bigdata.flink114.java.window.support.WordSplitFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 2个window join的例子
 * <p>
 * select a.*, b.* from a join b on a.id = b.id
 * 1、指定两张表
 * 2、指定这两张表的链接字段
 *
 * <pre>
 * stream.join(otherStream)        // 两个流进行关联
 *       .where(<KeySelector>)     //选择第一个流的key作为关联字段
 *       .equalTo(<KeySelector>)   //选择第二个流的key作为关联字段
 *       .window(<WindowAssigner>) //设置窗口的类型
 *       .apply(<JoinFunction>)    //对结果做操作 process apply = foreach
 * </pre>
 *
 * @author Roger.Yi
 */
public class WindowJoinSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source1、Source2
        DataStreamSource<String> source1 = env.socketTextStream("localhost", 6789);
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 5678);

        // Operator
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = source1.flatMap(new WordSplitFunction());
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds2 = source2.flatMap(new WordSplitFunction());

        // Join

        // run
        env.execute("WindowJoinSample");
    }
}
