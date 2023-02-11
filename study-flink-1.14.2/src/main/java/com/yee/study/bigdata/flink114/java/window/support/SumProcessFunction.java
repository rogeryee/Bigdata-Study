package com.yee.study.bigdata.flink114.java.window.support;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 提供了对于单词计算出现次数的自定义函数（类似sum作用）
 * <p>
 * 注意 ProcessWindowFunction 抽象类的四个泛型
 * IN 输入数据类型
 * OUT 输出数据类型
 * KEY key
 * W extends Window window类型
 *
 * @author Roger.Yi
 */
public class SumProcessFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

    private FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
        System.out.println("=================== A window is triggered ===================");
        System.out.println("Current SysTime: " + dateFormat.format(System.currentTimeMillis()));
        System.out.println("Current ProcessTime: " + dateFormat.format(context.currentProcessingTime()));
        System.out.println("Window StartTime: " + dateFormat.format(context.window().getStart()));
        System.out.println("Window EndTime: " + dateFormat.format(context.window().getEnd()));

        int count = 0;
        for (Tuple2<String, Integer> e : elements) {
            count++;
        }
        out.collect(Tuple2.of(key, count));
    }
}
