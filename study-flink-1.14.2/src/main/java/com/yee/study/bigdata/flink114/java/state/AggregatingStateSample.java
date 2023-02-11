package com.yee.study.bigdata.flink114.java.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * AggregatingState 示例
 * <p>
 * 需求： 不断输出每个 key 的 value 列表
 *
 * @author Roger.Yi
 */
public class AggregatingStateSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // Source
        DataStreamSource<Tuple2<Long, Long>> sourceDS = env.fromElements(
                Tuple2.of(1L, 3L),
                Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L),
                Tuple2.of(1L, 5L),
                Tuple2.of(2L, 3L),
                Tuple2.of(2L, 5L));

        // operator
        SingleOutputStreamOperator<Tuple2<Long, String>> resultDS = sourceDS.keyBy(0)
                .flatMap(new ContainsValueAggregatingState());

        // sink
        resultDS.print();

        // run
        env.execute("FlinkState_AggregatingState");
    }

    static class ContainsValueAggregatingState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, String>> {

        private AggregatingState<Long, String> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 自定义一个聚合逻辑
            AggregatingStateDescriptor desc = new AggregatingStateDescriptor<>("agg_state", new AggregateFunction<Long, String, String>() {
                @Override
                public String createAccumulator() {
                    return "值列表：";
                }

                // 分区内的计算
                // 假设依次接收到的值分别是 1 2 3
                // 值列表：1
                // 值列表：1 2
                // 值列表：1 2 3
                @Override
                public String add(Long value, String accumulator) {
                    if ("值列表：".equals(accumulator)) {
                        return accumulator + value;
                    } else {
                        return accumulator + ", " + value;
                    }
                }

                @Override
                public String getResult(String accumulator) {
                    return accumulator;
                }

                // 分区合并
                @Override
                public String merge(String a, String b) {
                    return a + ", " + b;
                }
            }, String.class);

            this.state = getRuntimeContext().getAggregatingState(desc);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, String>> out) throws Exception {
            this.state.add(value.f1);
            out.collect(Tuple2.of(value.f0, this.state.get()));
        }
    }
}
