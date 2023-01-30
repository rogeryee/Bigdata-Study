package com.yee.study.bigdata.flink.window;

import com.yee.study.bigdata.flink.window.support.WordSplitFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * 自定义 Evictor 清除器 示例
 * <p>
 * 需求：使用 Evictor 自己实现一个类似 CountWindow(3,2) 的效果，每隔 2个 单词计算最近 3个 单词
 * <p>
 * 驱逐器(evitor)可以在触发器触发之前或者之后，或者窗口函数被应用之前清理窗口中的元素。
 * <p>
 * Evitor接口有两个方法:
 * #evitorBefore()：包含了在window function之前被应用的驱逐逻辑
 * #evitorAfter()：包含了在window function之后被应用的驱逐逻辑。
 * 在 window function 应用之前被驱逐的元素将不会再被 window function 处理。
 * <p>
 * Flink有三个预实现的驱逐器:
 * 1) CountEvitor：在窗口中保持一个用户指定数量的元素，并在窗口的开始处丢弃剩余的其他元素
 * 2) DeltaEvitor: 通过一个DeltaFunction和一个阈值，计算窗口缓存中最近的一个元素和剩余的所有元素的delta值，并清除delta值大于或者等于阈值的元素
 * 3) TimeEvitor:使用一个interval的毫秒数作为参数，对于一个给定的窗口，它会找出元素中的最大时间戳max_ts，并清除时间戳小于max_tx - interval的元素。
 * 默认情况下:所有预实现的evitor都是在window function前应用它们的逻辑
 * 注意:指定一个Evitor要防止预聚合，因为窗口中的所有元素必须得在计算之前传递到驱逐器中
 * 注意:Flink 并不保证窗口中的元素是有序的，所以驱逐器可能从窗口的开始处清除，元素到达的先后不是那么必要。
 *
 * @author Roger.Yi
 */
public class EvictorSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source
        DataStreamSource<String> source = env.socketTextStream("localhost", 6789);

        // Operator
        // 一次输入 1 - 8，可以有 4 次输出
        // 1,2
        // 2,3,4
        // 4,5,6
        // 6,7,8
        source.flatMap(new WordSplitFunction())
              // 效果等同于 trigger + evictor
//              .countWindowAll(3, 2)
              // 这里不使用 keyed window，所有的输入都汇总在一起
              .windowAll(GlobalWindows.create())
              // 指定触发计算的规则，每接收到两个单词，就触发一次计算
              .trigger(CountTrigger.of(2))
              // 指定从窗口淘汰数据的规则， MyCountEvictor 维护这个窗口中，只有 3 个数据
              .evictor(new MyCountEvictor(3))
              .process(new ProcessAllWindowFunction<Tuple2<String, Integer>, String, GlobalWindow>() {
                  @Override
                  public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                      String word = "";
                      Iterator<Tuple2<String, Integer>> iterator = elements.iterator();
                      while (iterator.hasNext()) {
                          word += "," + iterator.next().f0;
                      }
                      out.collect(word.replaceFirst(",", ""));
                  }
              }).print().setParallelism(1);

        // run
        env.execute("EvictorSample");
    }

    /**
     * 自定义 Evictor
     * 泛型1： 窗口的输入数据的类型；Tuple2<String, Integer>
     * 泛型2： 窗口的类型
     * 假设现在 window 已经有 3 个元素了。 现在来2个元素，窗口里面有 5 个。
     * 但是我们只需要获取最近的 3 个来执行计算，所以需要 evict 一部分数据（最先进入 window 的数据）出去
     */
    private static class MyCountEvictor implements Evictor<Tuple2<String, Integer>, GlobalWindow> {
        // window 的大小
        private long windowCount;

        public MyCountEvictor(long windowCount) {
            this.windowCount = windowCount;
        }

        /**
         * 在 window 计算之前删除特定的数据
         *
         * @param elements       window 中所有的元素
         * @param size           window 中所有元素的大小
         * @param window         window
         * @param evictorContext 上下文
         */
        @Override
        public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window,
                                EvictorContext evictorContext) {
            // 如果当前的总的数据小于窗口的大小，就不需要删除数据了
            if (size <= windowCount) {
                return;
            } else {
                // 需要删除数据 1 2 3 4 5
                // 需要删除的数据的个数 淘汰：1 2 ， 留下来 3 4 5
                int evictorCount = 0;
                Iterator<TimestampedValue<Tuple2<String, Integer>>> iterator = elements.iterator();
                while (iterator.hasNext()) {
                    iterator.next();
                    evictorCount++;
                    // 满足这个条件，其实就是表示，窗口的数据条数，依然大于 windowCount
                    if (size - windowCount >= evictorCount) {
                        // 删除了元素
                        iterator.remove();
                    } else {
                        break;
                    }
                }
            }
        }

        /**
         * 在 window 计算之后删除特定的数据
         *
         * @param elements       window 中所有的元素
         * @param size           window 中所有元素的大小
         * @param window         window
         * @param evictorContext 上下文
         */
        @Override
        public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window,
                               EvictorContext evictorContext) {
        }
    }
}
