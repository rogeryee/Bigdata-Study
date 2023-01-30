package com.yee.study.bigdata.flink114.state;

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
 * ListState 示例
 * <p>
 * 数据输入：nc -lk 6789
 * 算子的并发度为 3
 * CASE1：依次输入1、2、3、4、5、6
 * 输入：1
 * 输出：1> 1
 * 输入：2
 * 输出：2> 2
 * 输入：3
 * 输出：3> 3
 * 输入：4
 * 输出：1> 1
 *      1> 4
 * 输入：5
 * 输出：2> 2
 *      2> 6
 * 输入：6
 * 输出：3> 3
 *      3> 6
 * 并发度为3，6次输入平均分配到了3个子算子中
 * </p>
 * CASE2：依次输入1、2、3、error、4、5、6
 * 输入：1
 * 输出：1> 1
 * 输入：2
 * 输出：2> 2
 * 输入：3
 * 输出：3> 3
 * 输入：error
 * 发生异常，并产生升checkpoint恢复，由于使用的是 ListState 所以各个子算子分别恢复自己的状态
 * 输出：数据恢复 -> 1
 *      数据恢复 -> 3
 *      数据恢复 -> 2
 * 输入：4
 * 输出：1> 1
 *      1> 4
 * 输入：5
 * 输出：2> 2
 *      2> 6
 * 输入：6
 * 输出：3> 3
 *      3> 6
 *
 * @author Roger.Yi
 */
public class ListStateSample {

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
                    .getListState(new ListStateDescriptor<String>("wordCount", String.class));
            this.state.get().forEach(e -> {
                // 数据恢复
                System.out.println("数据恢复 -> " + e);
                data.add(e);
            });
        }
    }
}
