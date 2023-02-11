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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * 使用 EventTime 作为处理时间（基于最大EventTime动态设置 watermark，全局并行度为2）
 * <p>
 * 需求：每隔5秒计算最近10秒的单词次数
 * <p>
 * 数据源（顺序输出，每隔10秒输出一条数据）
 * watermark允许5秒延迟
 * <p>
 * 16:44:10 并行度1：watermark(1970-01-01 07:59:55)
 * 16:44:10 并行度2：watermark(1970-01-01 07:59:55)
 * <p>
 * 16:44:20 并行度2：输出 1 条数据 event1（flink-1 eventTime=16:44:20）
 * 16:44:20 并行度1：watermark(1970-01-01 07:59:55)
 * 16:44:20 并行度2：watermark(16:44:15)
 * <p>
 * 16:44:30 并行度1：输出 1 条数据 event2（flink-2 eventTime=16:44:30）
 * 16:44:30 并行度1：watermark(16:44:25)
 * 16:44:30 并行度2：watermark(16:44:15)
 * <p>
 * 16:44:40 并行度1：输出 1 条数据 event3（flink-3 eventTime=16:23:08）
 * 16:44:40 watermark(16:23:03)
 *
 * @author Roger.Yi
 */
@Slf4j
public class TimeWindowEventTimeWithWatermarkSample5 {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Source
        DataStreamSource<MyEvent> source = env.addSource(new UnOrderedSource());

        // Operator
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = source.map(e -> e)
                                                                             .assignTimestampsAndWatermarks(
                                                                                     WatermarkStrategy
                                                                                             .forGenerator((ctx) -> new PeriodicWatermarkGenerator()) //watermark
                                                                                             .withTimestampAssigner((ctx) -> new MyEventTimestampExtractor())) //3)指定时间字段
                                                                             .keyBy(event -> event.getType())
                                                                             // 每隔 5s 计算过去 10s内 数据的结果。用的是 EventTime
                                                                             .window(SlidingEventTimeWindows.of(Time.seconds(10), Time
                                                                                     .seconds(5)))
                                                                             .process(new MyEventSumProcessFunction());

        operator.print();

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

            TimeUnit.SECONDS.sleep(10);
            Long time = System.currentTimeMillis();
            MyEvent event1 = new MyEvent("flink-1" + dateformat.format(time) + "),", time, "flink");
            cxt.collect(event1);

            TimeUnit.SECONDS.sleep(10);
            time = System.currentTimeMillis();
            MyEvent event2 = new MyEvent("flink-2" + dateformat.format(time) + "),", time, "flink");
            cxt.collect(event2);

            TimeUnit.SECONDS.sleep(10);
            time = System.currentTimeMillis();
            MyEvent event3 = new MyEvent("flink-3" + dateformat.format(time) + "),", time, "flink");
            cxt.collect(event3);

            TimeUnit.SECONDS.sleep(10);
        }

        @Override
        public void cancel() {
        }
    }
}
