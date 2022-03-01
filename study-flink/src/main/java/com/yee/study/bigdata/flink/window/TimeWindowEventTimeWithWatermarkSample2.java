package com.yee.study.bigdata.flink.window;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
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
 * 数据源（乱序输出）
 * 17:41:43 输出 1 条数据 event1（EventTime=17:41:43）
 * 17:41:46 输出 1 条数据 event4（EventTime=17:41:46）
 * 17:41:49 输出 1 条数据 event2（EventTime=17:41:43）
 * 17:41:51 输出 1 条数据 event3（EventTime=17:41:43）
 * <p>
 * event2 本该 17:41:43 输出，延迟到 17:41:49 输出
 * event3 本该 17:41:43 输出，延迟到 17:41:51 输出
 * <p>
 * 窗口日志：
 * 17:41:40  window [17:41:25 - 17:41:35] 窗口无数据，不触发计算
 * 17:41:45  window [17:41:30 - 17:41:40] 窗口无数据，不触发计算
 * 17:41:50  window [17:41:35 - 17:41:45] 包含 2 条数据（event1，event2），输出 (flink, 2)
 * 17:41:55  window [17:41:40 - 17:41:50] 包含 4 条数据（event1、event2、event3、event4），输出 (flink, 4)
 * 17:42:00  window [17:41:45 - 17:41:55] 包含 1 条数据（event4），输出 (flink, 1)
 * 17:42:05  window [17:41:50 - 17:42:00] 窗口无数据，不触发计算
 * <p>
 * 1. 每个窗口都会接受5s的延迟
 * 2. event2 会落在正确的窗口内
 * 3. event3 不能落在窗口 [17:41:35 - 17:41:45] 是因为它已经超过的5s的延迟
 *
 * @author Roger.Yi
 */
@Slf4j
public class TimeWindowEventTimeWithWatermarkSample2 {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
                TimeUnit.SECONDS.sleep(3);
            }
        }

        @Override
        public void cancel() {
        }
    }
}
