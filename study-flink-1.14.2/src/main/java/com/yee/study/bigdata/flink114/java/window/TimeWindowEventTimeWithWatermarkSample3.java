package com.yee.study.bigdata.flink114.java.window;

import com.yee.study.bigdata.flink114.java.window.support.MyEvent;
import com.yee.study.bigdata.flink114.java.window.support.MyEventSumProcessFunction;
import com.yee.study.bigdata.flink114.java.window.support.MyEventTimestampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * 使用 EventTime 作为处理时间（基于最大EventTime动态设置 watermark）
 * <p>
 * 需求：每隔5秒计算最近10秒的肖曦次数
 * <p>
 * 数据源（乱序输出，每隔3秒输出一条数据），watermark允许5秒延迟，并增加了 allowedLateness=3秒
 * 10:42:10 event1 生成并消费；event2、event3、event4 生成但是未消费
 * 10:42:22 event5 生成并消费
 * 10:42:22 event2 消费
 * 10:42:25 event6 生成并消费
 * 10:42:25 event3 消费
 * 10:42:28 event7 生成并消费
 * 10:42:28 event4 消费
 * 10:42:31 event8 生成并消费
 * 注：
 * 1). event2 延迟到 10:42:22 才消费
 * 2). event3 延迟到 10:42:25 才消费
 * 3). event4 延迟到 10:42:28 才消费
 * <p>
 * 10:42:10 输出 1 条数据 event1（flink-1 eventTime=10:42:10）
 * 10:42:10 watermark(10:42:05)
 * <p>
 * 10:42:22 输出 1 条数据 event5（flink-5 eventTime=10:42:22）
 * 10:42:22 输出 2 条数据 event2（flink-2 eventTime=10:42:10）
 * 10:42:22 watermark(10:42:17) => Window[10:42:05 - 10:42:15] 触发(watermark > 10:42:15)，输出 (flink,2)，包含2条数据（event1、event2）
 * <p>
 * 10:42:25 输出 1 条数据 event6（flink-6 eventTime=10:42:25）
 * 10:42:25 输出 1 条数据 event3（flink-3 eventTime=10:42:10）=> Window[10:42:05 - 10:42:15] 触发(watermark=10:42:17 >= 10:42:15 - 1s 且 watermark <= 10:42:15 - 1ms + allowLateness)，输出 (flink,3)，包含3条数据（event1、event2、event3）
 * 10:42:25 watermark(10:42:20) => Window[10:42:10 - 10:42:20] 触发(watermark > 10:42:20 - 1ms)，输出 (flink,3)，包含3条数据（event1、event2、event3）
 * <p>
 * 10:42:28 输出 1 条数据 event7（flink-7 eventTime=10:42:28）
 * 10:42:28 输出 1 条数据 event4（flink-4 eventTime=10:42:10）=> Window[10:42:10 - 10:42:20] 触发(watermark=10:42:20 > 10:42:20 - 1ms 且 watermark <= 10:42:20 - 1ms + allowLateness)，输出 (flink,4)，包含4条数据（event1、event2、event3、event4）
 * 10:42:28 watermark(10:42:23)
 * <p>
 * 10:42:31 输出 1 条数据 event8（flink-8 eventTime=10:42:31）
 * 10:42:31 watermark(10:42:26)
 * <p>
 *
 * @author Roger.Yi
 */
@Slf4j
public class TimeWindowEventTimeWithWatermarkSample3 {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // env.getConfig().setAutoWatermarkInterval(20000); 设置watermark频率，默认 200ms

        // Source
        DataStreamSource<MyEvent> source = env.addSource(new UnOrderedSource()).setParallelism(1);

        // Operator
        source.map(e -> e)
              .assignTimestampsAndWatermarks(
                      WatermarkStrategy
                              .forGenerator((ctx) -> new PeriodicWatermarkGenerator()) //watermark
                              .withTimestampAssigner((ctx) -> new MyEventTimestampExtractor())) // 指定时间字段
              .keyBy(MyEvent::getType)
              // 每隔 5s 计算过去 10s内 数据的结果。用的是 EventTime
              .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
              .allowedLateness(Time.seconds(3))
              .process(new MyEventSumProcessFunction())
              .print().setParallelism(1);

        env.execute("TimeWindowWithUnOrderedSourceByEventTimeSample");
    }

    /**
     * 指定时间字段
     */
    static class PeriodicWatermarkGenerator implements WatermarkGenerator<MyEvent>, Serializable {

        private FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

        // 当前窗口里面的最大的事件时间。
        private long currentMaxEventTime = 0L;

        // 最大允许的乱序时间 5 秒
        private long maxOutOfOrderness = 5000;

        @Override
        public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
            // 比较 当前的事件时间 和 原有最大的事件时间，取两者中较大的
            this.currentMaxEventTime = Math.max(event.getEventTime(), currentMaxEventTime);
            System.out.println(dateFormat.format(System.currentTimeMillis()) + " onEvent: event=" + event + ", currentMaxEventTime=" + dateFormat
                    .format(this.currentMaxEventTime));
        }

        // 支持延迟5s的数据
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(currentMaxEventTime - maxOutOfOrderness));
            System.out.println(dateFormat.format(System.currentTimeMillis()) + " onPeriodicEmit: watermark=" + dateFormat
                    .format(currentMaxEventTime - maxOutOfOrderness));
        }
    }

    /**
     * 自定义的顺序 source
     */
    static class UnOrderedSource implements SourceFunction<MyEvent> {

        private FastDateFormat dateformat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void run(SourceContext<MyEvent> cxt) throws Exception {
            // 为了保证是 10s 的倍数。
            String currTime = String.valueOf(System.currentTimeMillis());
            while (Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100) {
                currTime = String.valueOf(System.currentTimeMillis());
                continue;
            }

            log.info("当前时间：" + dateformat.format(System.currentTimeMillis()));

            // 10s
            Long time = System.currentTimeMillis();
            MyEvent event1 = new MyEvent("flink-1 (" + dateformat.format(time) + "),", time, "flink");
            cxt.collect(event1);
            TimeUnit.SECONDS.sleep(3);

            MyEvent event2 = new MyEvent("flink-2 (" + dateformat.format(time) + "),", time, "flink");
            TimeUnit.SECONDS.sleep(3);

            MyEvent event3 = new MyEvent("flink-3 (" + dateformat.format(time) + "),", time, "flink");
            TimeUnit.SECONDS.sleep(3);

            MyEvent event4 = new MyEvent("flink-4 (" + dateformat.format(time) + "),", time, "flink");
            TimeUnit.SECONDS.sleep(3);

            // 22s
            time = System.currentTimeMillis();
            MyEvent event5 = new MyEvent("flink-5 (" + dateformat.format(time) + "),", time, "flink");
            cxt.collect(event5);
            cxt.collect(event2);
            TimeUnit.SECONDS.sleep(3);

            // 25s
            time = System.currentTimeMillis();
            MyEvent event6 = new MyEvent("flink-6 (" + dateformat.format(time) + "),", time, "flink");
            cxt.collect(event6);
            cxt.collect(event3);
            TimeUnit.SECONDS.sleep(3);

            // 28s
            time = System.currentTimeMillis();
            MyEvent event7 = new MyEvent("flink-7 (" + dateformat.format(time) + "),", time, "flink");
            cxt.collect(event7);
            cxt.collect(event4);
            TimeUnit.SECONDS.sleep(3);

            // 31s
            time = System.currentTimeMillis();
            MyEvent event8 = new MyEvent("flink-8 (" + dateformat.format(time) + "),", time, "flink");
            cxt.collect(event8);

            TimeUnit.SECONDS.sleep(300000);
        }

        @Override
        public void cancel() {
        }
    }
}
