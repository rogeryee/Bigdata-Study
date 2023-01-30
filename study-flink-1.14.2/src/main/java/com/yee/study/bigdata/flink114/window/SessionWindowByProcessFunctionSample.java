package com.yee.study.bigdata.flink114.window;

import com.yee.study.bigdata.flink114.window.support.WordSplitFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 基于 State 和自定义 Function 实现类似 SessionWindow 的示例
 * <p>
 * 需求： 5秒 过去以后，该单词不出现就打印出来该单词和它的出现次数
 *
 * @author Roger.Yi
 */
public class SessionWindowByProcessFunctionSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Source
        DataStreamSource<String> source = env.socketTextStream("localhost", 6789);

        // Operator
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds = source
                .flatMap(new WordSplitFunction())
                .keyBy(tuple -> tuple.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

                    private ValueState<Tuple3<String, Integer, Long>> wordState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Tuple3<String, Integer, Long>> desc = new ValueStateDescriptor<>("word state",
                                                                                                              Types.TUPLE(Types.STRING, Types.INT, Types.LONG));
                        this.wordState = getRuntimeContext().getState(desc);
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Tuple3<String, Integer, Long> currentState = this.wordState.value();
                        if (currentState == null) {
                            currentState = Tuple3.of(value.f0, 0, 0L);
                        }

                        currentState.f1 += 1;
                        currentState.f2 = System.currentTimeMillis();
                        this.wordState.update(currentState);

                        // 注册一个定时器： 注册一个以 Processing Time 为准的定时器
                        // 如果触发了定时器任务的话，就会调用 onTimer 方法来触发计算
                        // 定时器触发的时间是当前 key 的最后修改时间加上 5 秒
                        ctx.timerService().registerProcessingTimeTimer(currentState.f2 + 5000);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Tuple3<String, Integer, Long> currentState = this.wordState.value();
                        if (currentState != null) {
                            out.collect(Tuple2.of(currentState.f0, currentState.f1));
                            this.wordState.clear();
                        }
                    }
                });

        // Sink
        ds.print().setParallelism(1);

        // run
        env.execute("SessionWindowSample");
    }
}
