package com.yee.study.bigdata.flink.window;

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
 * 使用 EventTime 作为处理时间
 * <p>
 * 需求：每隔5秒计算最近10秒的单词次数
 * <p>
 * 数据源（顺序输出）
 * 11:17:43 输出 1 条数据 event1（EventTime=11:17:43, ProcessTime=11:17:43）
 * 11:17:46 输出 1 条数据 event2（EventTime=11:17:43, ProcessTime=11:17:43）
 * 11:17:49 输出 1 条数据 event3（EventTime=11:17:43, ProcessTime=11:17:43）
 * <p>
 * 第二条数据虽然是 11:17:46 产生的，但是用于窗口统计则应该使用 11:17:43 （EventTime）
 * <p>
 * 窗口日志：
 * 11:17:35  window [11:17:25 - 11:17:35] 窗口无数据，不触发计算
 * 11:17:40  window [11:17:30 - 11:17:40] 窗口无数据，不触发计算
 * 11:17:45  window [11:17:35 - 11:17:45] 包含 1 条数据（event1），输出 (flink, 1)
 * 11:17:50  window [11:17:40 - 11:17:50] 包含 3 条数据（event1、event2、event3），输出 (flink, 3)
 * 11:17:55  window [11:17:45 - 11:17:55] 包含 1 条数据（只包含event3，不包含event2，因为event2的 EventTime不在当前窗口），输出 (flink, 1)
 * 11:18:00  window [11:17:50 - 11:18:00] 窗口无数据，不触发计算
 *
 * @author Roger.Yi
 */
public class TimeWindowEventTimeWithOrderedSourceSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source
        DataStreamSource<MyEvent> source = env.addSource(new UnOrderedSource());

        // Operator
        source.map(e -> e)
              .assignTimestampsAndWatermarks(
                      WatermarkStrategy
                              .forGenerator((ctx) -> new PeriodicWatermarkGenerator()) //watermark
                              .withTimestampAssigner((ctx) -> new TimestampExtractor())) //3)指定时间字段
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

        @Override
        public void onEvent(MyEvent event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("onEvent: event=" + event + ", eventTimestamp=" + eventTimestamp);
        }

        // 不考虑延迟数据（使用当前时间作为 watermark）
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(System.currentTimeMillis()));
        }
    }

    /**
     * 定义EventTime字段获取方式
     */
    static class TimestampExtractor implements TimestampAssigner<MyEvent> {
        @Override
        public long extractTimestamp(MyEvent event, long recordTimestamp) {
            System.out.println("TimestampExtractor: " + event);
            return event.getEventTime();
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

            System.out.println("当前时间：" + dateformat.format(System.currentTimeMillis()));

            // 13s 输出一条数据
            TimeUnit.SECONDS.sleep(13);
            Long time = System.currentTimeMillis();
            MyEvent event1 = new MyEvent("flink-1(" + dateformat.format(time) + "),", time, "flink");
            cxt.collect(event1);

            MyEvent event2 = new MyEvent("flink-2(" + dateformat.format(time) + "),", time, "flink");
            TimeUnit.SECONDS.sleep(3);
            cxt.collect(event2);

            // 16s 输出一条数据
            TimeUnit.SECONDS.sleep(3);
            time = System.currentTimeMillis();
            MyEvent event3 = new MyEvent("flink-3(" + dateformat.format(time) + "),", time, "flink");
            cxt.collect(event3);

            // 本该 13s 输出的一条数据，延迟到 19s 的时候才输出
//            TimeUnit.SECONDS.sleep(3);
//            cxt.collect(event2);

            TimeUnit.SECONDS.sleep(30000000);
        }

        @Override
        public void cancel() {
        }
    }
}
