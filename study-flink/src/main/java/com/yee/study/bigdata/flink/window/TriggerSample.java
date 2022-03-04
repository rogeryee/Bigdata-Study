package com.yee.study.bigdata.flink.window;

import com.yee.study.bigdata.flink.window.support.WordSplitFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

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
                .trigger(new MyCountTrigger(3))
                .sum(1);

        // Sink
        ds.print().setParallelism(1);

        // run
        env.execute("TriggerSample");
    }

    /**
     * 自定义触发器: 每三个元素计算一次
     * Tuple2<String, Integer> 输入的数据类型
     * GlobalWindow  窗口的数据类型
     * 有 4 个抽象方法，需要去实现： 其中 onElement 表示每次接收到这个 window 的一个输入，就调用一次
     */
    private static class MyCountTrigger extends Trigger<Tuple2<String, Integer>, GlobalWindow> {
        // 指定元素的最大数量
        private long maxCount;

        // 用于存储每个 key 对应的 count 值
        private ReducingStateDescriptor<Long> desc = new ReducingStateDescriptor<Long>("count", new ReduceFunction<Long>() {
            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }
        }, Long.class);

        private MyCountTrigger(long maxCount) {
            this.maxCount = maxCount;
        }

        /**
         * 当一个元素进入到一个 window 中的时候就会调用这个方法
         * Tuple2<String, Integer> element = <a,1>
         *
         * @param element   元素
         * @param timestamp 进来的时间
         * @param window    元素所属的窗口
         * @param ctx       上下文
         * @return TriggerResult
         * 1. TriggerResult.CONTINUE ：表示对 window 不做任何处理
         * 2. TriggerResult.FIRE ：表示触发 window 的计算
         * 3. TriggerResult.PURGE ：表示清除 window 中的所有数据
         * 4. TriggerResult.FIRE_AND_PURGE ：表示先触发 window 计算，然后删除 window 中的数据
         */
        @Override
        public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
            // 拿到当前 key 对应的 count 状态值
            ReducingState<Long> count = ctx.getPartitionedState(desc);

            // count 累加 1
            count.add(1L);

            // 如果当前 key 的 count 值等于 maxCount
            // 单词出现了三次才输出
            // 接收到三个输入才输出
            if (count.get() == maxCount) {
                // ReducingState 清空
                count.clear();
                // 触发 window 计算，删除数据
                // 清空整个窗口的数据
                return TriggerResult.FIRE_AND_PURGE;
            } else {
                // 否则，对 window 不做任何的处理
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            // 基于 Processing Time 的定时器任务逻辑
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            // 基于 Event Time 的定时器任务逻辑
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(desc).clear();
        }
    }
}