package com.yee.study.bigdata.flink114.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * ValueState 示例
 * <p>
 * 需求：每 3 个相同 key 就输出他们的 平均值
 * 输入数据：
 * Tuple2.of(1L, 3L),
 * Tuple2.of(1L, 7L),
 * Tuple2.of(2L, 4L),
 * Tuple2.of(1L, 5L),
 * Tuple2.of(2L, 3L),
 * Tuple2.of(2L, 5L)
 * 输出：
 * (1,5.0)
 * (2,4.0)
 * -
 * 注意：一个 key 一个 valuestate
 *
 * @author Roger.Yi
 */
public class ValueStateSample {

    public static void main(String[] args) throws Exception {
        // ENV
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // Source
        DataStreamSource<Tuple2<Long, Long>> source = env.fromElements(
                Tuple2.of(1L, 3L),
                Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L),
                Tuple2.of(1L, 5L),
                Tuple2.of(2L, 3L),
                Tuple2.of(2L, 5L));

        // 计算
        SingleOutputStreamOperator<Tuple2<Long, Double>> ds = source.keyBy(0)
                .flatMap(new CountAverageWithValueState());

        // Sink
        ds.print();

        env.execute("ValueStateSample");
    }

    /**
     * Value State 自定义实现
     *
     * 注意：一个 key 一个 ValueState
     */
    static class CountAverageWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

        // Value state 成员变量 (某个key出现的次数和总和)
        private ValueState<Tuple2<Long, Long>> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<Long, Long>> stateDesc = new ValueStateDescriptor<Tuple2<Long, Long>>("countAndSumState",
                    Types.TUPLE(Types.LONG, Types.LONG));
            this.state = getRuntimeContext().getState(stateDesc);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> collector) throws Exception {
            Tuple2<Long, Long> currentState = this.state.value();
            if (currentState == null) {
                currentState = Tuple2.of(0L, 0L);
            }

            // 更新状态（key + 1， value + valueNew）
            currentState.f0 += 1;
            currentState.f1 += value.f1;
            this.state.update(currentState);

            // 判断是否满足需求
            if (currentState.f0 == 3) {
                // 求平均
                double avg = currentState.f1 / currentState.f0;
                collector.collect(Tuple2.of(value.f0, avg));
                // 清空状态
                this.state.clear();
            }
        }
    }
}
