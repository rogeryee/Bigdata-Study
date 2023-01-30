package com.yee.study.bigdata.flink.window.support;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 针对 MyEvent 类的处理函数
 *
 * @author Roger.Yi
 */
@Slf4j
public class MyEventSumProcessFunction extends ProcessWindowFunction<MyEvent, Tuple2<String, Integer>, String, TimeWindow> {

    private FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void process(String key, Context context, Iterable<MyEvent> allElements,
                        Collector<Tuple2<String, Integer>> out) throws Exception {
        int count = 0;
        for (MyEvent e : allElements) {
            count++;
        }
        out.collect(Tuple2.of(key, count));

        String winStart = dateFormat.format(context.window().getStart());
        String winEnd = dateFormat.format(context.window().getEnd());
        String processTime = dateFormat.format(context.currentProcessingTime());
        String currentTime = dateFormat.format(System.currentTimeMillis());
        log.info(currentTime + ": processTime = " + processTime + " Window[" + winStart + " - " + winEnd + "] triggered, out=(" + key + ", " + count + "), events=" + allElements);
    }
}
