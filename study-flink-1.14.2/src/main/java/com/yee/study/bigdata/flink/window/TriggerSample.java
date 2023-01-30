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
 * 自定义 Trigger 触发器 示例
 * <p>
 * 需求：使用 Trigger 自己实现一个类似 CountWindow 的效果
 * 具体实现： countWindow(3)
 * 1、这个单词出现了 3 次就输出一次。wordcount 需求下，能看到这样的效果
 * 2、这个 key 的 window 内部有三个元素了，就触发计算，做输出
 * <p>
 * 触发器决定了一个窗口何时可以被窗口函数处理，每一个窗口分配器都有一个默认的触发器。
 * <p>
 * 触发器的接口有5个方法来允许触发器处理不同的事件:
 * #onElement()：每个元素被添加到窗口时调用
 * #onEventTime()：当一个已注册的事件时间计时器启动时调用
 * #onProcessingTime()：当一个已注册的处理时间计时器启动时调用
 * #onMerge()：与状态性触发器相关，当使用会话窗口时，两个触发器对应的窗口合并时，合并两个触发器的状态。
 * #clear()：执行任何需要清除的相应窗口
 * <p>
 * 注意:
 * 1)第一、三通过返回一个 TriggerResult 来决定如何操作调用他们的事件，这些操作可以是下面操作中的一个；
 * CONTINUE: 什么也不做
 * FIRE: 触发计算
 * PURGE: 清除窗口中的数据
 * FIRE_AND_PURGE: 触发计算并清除窗口中的数据
 * 2) 这些函数可以被用来为后续的操作注册处理时间定时器或者事件时间计时器
 * <p>
 * 触发和清除(Fire and Purge)
 * 一旦一个触发器决定一个窗口已经准备好进行处理，它将触发并返回FIRE或者FIRE_AND_PURGE。
 * 这是窗口操作发送当前窗口结果的信号，给定一个拥有一个 WindowFunction 的窗口那么所有的元素都将发送到 WindowFunction 中(可能之后还会发送到驱逐器(Evitor)中)。
 * 有ReduceFunction或者FoldFunction的Window仅仅发送他们的急切聚合结果。
 * 当一个触发器触发时，它可以是FIRE或者FIRE_AND_PURGE，如果是FIRE的话，将保持window中的内容，FIRE_AND_PURGE的话，会清除window的内容。默认情况下，预实现的触发器仅仅是FIRE，不会清除window的状态。
 * 注意:清除操作仅清除window的内容，并留下潜在的窗口元信息和完整的触发器状态。
 * <p>
 * 窗口分配器默认的触发器(Default Triggers of WindowAssigners)
 * 默认的触发器适用于许多种情况，例如:所有的事件时间分配器都有一个EventTimeTrigger作为默认的触发器，这个触发器仅在当水印通过窗口的最后时间时触发。
 * 注意:GlobalWindow 默认的触发器是 NeverTrigger，是永远不会触发的，因此，如果你使用的是GlobalWindow的话，你需要定义一个自定义触发器。
 * 注意:通过调用trigger(...)来指定一个触发器你就重写了 WindowAssigner 的默认触发器。例如:如果你为TumblingEventTimeWindows指定了一个CountTrigger，你就不会再通过时间来获取触发了，而是通过计数。现在，如果你想通过时间和计数来触发的话，你需要写你自己自定义的触发器。
 * <p>
 * 内置的和自定义的触发器(Build-in and Custom Triggers)
 * Flink有一些内置的触发器:
 * EventTimeTrigger(前面提到过)触发是根据由水印衡量的事件时间的进度来的
 * ProcessingTimeTrigger 根据处理时间来触发
 * CountTrigger 一旦窗口中的元素个数超出了给定的限制就会触发
 * PurgingTrigger 作为另一个触发器的参数并将它转换成一个清除类型
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