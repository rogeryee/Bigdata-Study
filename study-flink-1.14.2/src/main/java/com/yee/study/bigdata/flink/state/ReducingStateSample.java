package com.yee.study.bigdata.flink.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ReducingState 示例
 * <p>
 * 累积求和（不使用 sum 算子来完成 sum 的逻辑处理）
 *
 * @author Roger.Yi
 */
public class ReducingStateSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // Source
        DataStreamSource<Tuple2<Long, Long>> sourceDS = env.fromElements(
                Tuple2.of(1L, 3L),
                Tuple2.of(1L, 7L),
                Tuple2.of(1L, 5L), Tuple2.of(2L, 4L), Tuple2.of(2L, 3L), Tuple2.of(2L, 5L));

        // operator
        SingleOutputStreamOperator<Tuple2<Long, Long>> resultDS = sourceDS.keyBy(0)
                .flatMap(new SumByReducingStateFunction());

        // sink
        resultDS.print();

        // run
        env.execute("FlinkState_ReducingState");
    }

    /**
     * 自定义的用来实现累积求和效果的聚合函数
     * oldState + input = newState
     */
    static class SumByReducingStateFunction extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private ReducingState<Long> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 在初始化 ReducingState 定义了一个聚合函数
            ReducingStateDescriptor<Long> descriptor = new ReducingStateDescriptor<Long>("reduce_state", new ReduceFunction<Long>() {
                @Override
                public Long reduce(Long value1, Long value2) throws Exception {
                    return value1 + value2;
                }
            }, Long.class);

            this.state = getRuntimeContext().getReducingState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
            this.state.add(value.f1);
            out.collect(Tuple2.of(value.f0, this.state.get()));
        }
    }
}
