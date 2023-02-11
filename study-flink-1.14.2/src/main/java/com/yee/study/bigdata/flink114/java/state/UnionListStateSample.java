package com.yee.study.bigdata.flink114.java.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * UnionListState 示例
 * 与 ListState 的区别在于，在发生故障时，或者从保存点（savepoint）启动应用程序时如何恢复
 * 使用UnionListState 并行度是多少，会恢复多少份数据。
 * <p>
 * 数据输入：nc -lk 6789
 * 算子的并发度为 3
 * CASE1：依次输入1、2、3、error、4、5、6
 * 输入：1
 * 输出：1> 1
 * 输入：2
 * 输出：2> 2
 * 输入：3
 * 输出：3> 3
 * 输入：error
 * 发生异常，并产生升checkpoint恢复，由于使用的是 UnionListState 且并行度为 3，所以总共恢复了3套数据
 * 输出：数据恢复 -> 1
 *      数据恢复 -> 3
 *      数据恢复 -> 2
 *      数据恢复 -> 1
 *      数据恢复 -> 3
 *      数据恢复 -> 2
 *      数据恢复 -> 1
 *      数据恢复 -> 3
 *      数据恢复 -> 2
 * 输入：4
 * 输出：1> 1
 *      1> 4
 *      1> 2
 *      1> 3
 * 输入：5
 * 输出：2> 2
 *      2> 6
 *      2> 1
 *      2> 3
 * 输入：6
 * 输出：3> 3
 *      3> 6
 *      3> 1
 *      3> 2
 * 故障恢复后，每个子算子都获取了全部的状态（包括其他2个子算子的）
 *
 * @author Roger.Yi
 */
public class UnionListStateSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(3000);

        // Source
        DataStreamSource<String> source = env.socketTextStream("localhost", 6789);

        // 计算
        SingleOutputStreamOperator<String> ds = source.flatMap(new CountWithListState()).returns(Types.STRING);

        // Sink
        ds.print();

        // run
        env.execute("StateOperator");
    }

    static class CountWithListState implements FlatMapFunction<String, String>, CheckpointedFunction {

        // 数据
        private List<String> data = new ArrayList<>();

        // 数据状态
        private ListState<String> state;

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            // 模拟异常
            if ("error".equals(value)) {
                throw new RuntimeException("程序出错了....");
            }

            Arrays.stream(value.split(" ")).forEach(s -> data.add(s));
            data.forEach(out::collect);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            this.state.clear(); // 清空状态
            this.state.addAll(this.data); //保存状态
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            this.state = context.getOperatorStateStore()
                    .getUnionListState(new ListStateDescriptor<String>("wordCount", String.class));
            this.state.get().forEach(e -> {
                // 数据恢复
                System.out.println("数据恢复 -> " + e);
                data.add(e);
            });
        }
    }
}
