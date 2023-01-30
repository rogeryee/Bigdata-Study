package com.yee.study.bigdata.flink114.window;

import com.yee.study.bigdata.flink114.window.support.MyEvent;
import com.yee.study.bigdata.flink114.window.support.MyEventSumProcessFunction;
import com.yee.study.bigdata.flink114.window.support.MyEventTimestampExtractor;
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
 * 需求：每隔5秒计算最近10秒的单词次数
 * <p>
 * 数据源（顺序输出，每隔4秒输出一条数据）
 * watermark允许5秒延迟
 * <p>
 * 16:23:00 输出 1 条数据 event1（flink-1 eventTime=16:23:00）
 * 16:23:00 watermark(16:22:55)
 * <p>
 * 16:23:04 输出 1 条数据 event2（flink-2 eventTime=16:23:04）
 * 16:23:04 watermark(16:22:59)
 * <p>
 * 16:23:08 输出 1 条数据 event3（flink-3 eventTime=16:23:08）
 * 16:23:08 watermark(16:23:03)
 * <p>
 * 16:23:12 输出 1 条数据 event4（flink-4 eventTime=16:23:12）
 * 16:23:12 watermark(16:23:07)
 * 16:23:12 Window[16:22:55 - 16:23:05] 触发(watermark > 16:23:05)，输出 (flink,2)，包含2条数据（event1、event2）
 * <p>
 * 16:23:16 输出 1 条数据 event5（flink-5 eventTime=16:23:16）
 * 16:23:16 watermark(16:23:11)
 * 16:23:16 Window[16:23:00 - 16:23:10] 触发(watermark > 16:23:10)，输出 (flink,3)，包含2条数据（event1、event2、event3）
 * <p>
 * 16:23:20 输出 1 条数据 event6（flink-6 eventTime=16:23:20
 * 16:23:20 watermark(16:23:15)
 * 16:23:20 Window[16:23:05 - 16:23:15] 触发(watermark > 16:23:15)，输出 (flink,2)，包含2条数据（event3、event4）
 * <p>
 * 16:23:24 输出 1 条数据 event7（flink-7 eventTime=16:23:24）
 * 16:23:24 watermark(16:23:19)
 * <p>
 * 16:23:28 输出 1 条数据 event8（flink-8 eventTime=16:23:28
 * 16:23:28 watermark(16:23:23)
 * 16:23:28 Window[16:23:10 - 16:23:20] 触发(watermark > 16:23:20)，输出 (flink,2)，包含2条数据（event4、event5）
 *
 * @author Roger.Yi
 */
@Slf4j
public class TimeWindowEventTimeWithWatermarkSample2 {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(2000); // 设置每 2s 调用一次 onPeriodicEmit 方法

        // Source
        DataStreamSource<MyEvent> source = env.addSource(new UnOrderedSource()).setParallelism(1);

        // Operator
        source.map(e -> e)
              .assignTimestampsAndWatermarks(
                      WatermarkStrategy
                              .forGenerator((ctx) -> new PeriodicWatermarkGenerator()) //watermark
                              .withTimestampAssigner((ctx) -> new MyEventTimestampExtractor())) //3)指定时间字段
              .keyBy(event -> event.getType())
              // 每隔 5s 计算过去 10s内 数据的结果。用的是 EventTime
              .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
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

        // 最大允许的乱序时间 10 秒
        private long maxOutOfOrderness = 5000;

        @Override
        public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
            // 比较 当前的事件时间 和 原有最大的事件时间，取两者中较大的
            this.currentMaxEventTime = Math.max(event.getEventTime(), currentMaxEventTime);
            log.info("onEvent: event=" + event + ", currentMaxEventTime=" + dateFormat.format(this.currentMaxEventTime));
        }

        // 支持延迟5s的数据
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(currentMaxEventTime - maxOutOfOrderness));
            log.info("onPeriodicEmit: watermark=" + dateFormat.format(currentMaxEventTime - maxOutOfOrderness));
        }
    }

    /**
     * 注释： 自定义的顺序 source
     * 1、在第 13s 的时候，输出两条数据
     * 2、在第 16s 的时候，输出一条数据
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

            int i = 1;
            while (true) {
                Long time = System.currentTimeMillis();
                MyEvent event = new MyEvent("flink-" + (i++) + " (" + dateformat.format(time) + "),", time, "flink");
                cxt.collect(event);
                TimeUnit.SECONDS.sleep(4);
            }
        }

        @Override
        public void cancel() {
        }
    }
}
