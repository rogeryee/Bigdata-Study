package com.yee.study.bigdata.flink114.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * OperatorState 示例
 * <p>
 * 需求：每 N 条数据打印输出一次
 * <p>
 * KeyedState vs. OperatorState
 * 1、keyedState ： 一个 key 一个 state(List， Value, Map,.....)
 * 2、OperatorState ：  一个 Task 一个 state
 *
 * @author Roger.Yi
 */
public class StateSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source
        DataStreamSource<Tuple2<String, Integer>> source = env.fromElements(
                Tuple2.of("Spark", 3),
                Tuple2.of("Flink", 5),
                Tuple2.of("Hadoop", 7),
                Tuple2.of("Hive", 6),
                Tuple2.of("HBase", 9),
                Tuple2.of("Spark", 4));

        // sink
        // Sink Operator 的每一个 Task维护一个 ListState
        // 设置并行度为1，则全局一个State，输出 (3, 5), (7, 6)
        // 设置并行度为2，则每个并行度一个State，并行1输出 (3, 7) 并行2输出 (5, 6)
        source.addSink(new PrintSink(2)).setParallelism(2);

        // run
        env.execute("StateOperator");
    }

    /**
     * 自定义状态管理
     * <p>
     * CheckpointedFunction 提供了
     * 1、持久化的方法： 把 state 存起来
     * 2、恢复的方法： 从 state 的存储地 再恢复回来
     */
    public static class PrintSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

        // 每 N 条输出
        private int recordNumber;

        // 数据容器
        private List<Tuple2<String, Integer>> bufferElements;

        // ListState 状态
        private ListState<Tuple2<String, Integer>> listState;

        public PrintSink(int recordNumber) {
            this.recordNumber = recordNumber;
            this.bufferElements = new ArrayList<>();
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            this.bufferElements.add(value);
            if (bufferElements.size() == this.recordNumber) {
                System.out.println("自定义输出: " + bufferElements);
                this.bufferElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            this.listState.clear();
            for (Tuple2<String, Integer> e : bufferElements) {
                this.listState.add(e);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Tuple2<String, Integer>> listStateDescriptor = new ListStateDescriptor<Tuple2<String, Integer>>("PrintSink", TypeInformation
                    .of(new TypeHint<Tuple2<String, Integer>>() {
                    }));

            this.listState = context.getOperatorStateStore().getListState(listStateDescriptor);

            if (context.isRestored()) {
                for (Tuple2<String, Integer> e : listState.get()) {
                    this.bufferElements.add(e);
                }
            }
        }
    }
}
