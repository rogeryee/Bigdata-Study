package com.yee.study.bigdata.flink.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * 自定义数据源：乱序输出 使用 EventTime
 * 第13秒输出2条数据；
 * 第16秒输出 1 条数据；
 * <p>
 * 需求：每隔5秒计算最近10秒的单词次数
 * <p>
 * 数据源：
 * 17:33:13 输出1条数据
 * 17:33:16 输出1条数据
 * 17:33:19 输出1条数据
 * <p>
 * 处理端：
 * 17:33:00  不触发 window 计算
 * 17:33:05  不触发 window 计算
 * 17:33:10  不触发 window 计算
 * 17:33:15  [17:33:05 - 17:33:15] 输出 (flink, 1)
 * 17:33:20  [17:33:10 - 17:33:20] 输出 (flink, 3)
 * 17:33:25  [17:33:15 - 17:33:25] 输出 (flink, 1)
 * 17:33:30  不触发 window 计算
 *
 * @author Roger.Yi
 */
public class TimeWindowWithUnOrderedSourceByEventTimeSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source
        DataStreamSource<String> source = env.addSource(new UnOrderedSource());

        // Operator
        source.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String input, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] word = input.split(",");
                String value = word[0];
                String eventTime = word[1];
                out.collect(Tuple2.of(value, Long.valueOf(eventTime)));
            }
        })
              .assignTimestampsAndWatermarks(
                      WatermarkStrategy
                              .forGenerator((ctx) -> new PeriodicWatermarkGenerator()) //watermark
                              .withTimestampAssigner((ctx) -> new TimestampExtractor())) //3)指定时间字段
              .keyBy(tuple -> tuple.f0)
              // 每隔 5s 计算过去 10s内 数据的结果。用的是 ProcessingTime
              .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
              .process(new LongSumProcessFunction())
              .print().setParallelism(1);

        env.execute("TimeWindowWithUnOrderedSourceByEventTimeSample");
    }

    static class PeriodicWatermarkGenerator implements WatermarkGenerator<Tuple2<String, Long>>, Serializable {

        @Override
        public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
            System.out.println(eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(System.currentTimeMillis()));
        }
    }

    /**
     * 定义EventTime字段获取方式
     */
    static class TimestampExtractor implements TimestampAssigner<Tuple2<String, Long>> {
        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
            return element.f1;
        }
    }

    /**
     * 注释： 自定义的顺序 source
     * 1、在第 13s 的时候，输出两条数据
     * 2、在第 16s 的时候，输出一条数据
     */
    static class UnOrderedSource implements SourceFunction<String> {

        private FastDateFormat dateformat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void run(SourceContext<String> cxt) throws Exception {
            // 为了保证是 10s 的倍数。
            String currTime = String.valueOf(System.currentTimeMillis());
            while (Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100) {
                currTime = String.valueOf(System.currentTimeMillis());
                continue;
            }

            System.out.println("当前时间：" + dateformat.format(System.currentTimeMillis()));

            // 13s 输出一条数据
            TimeUnit.SECONDS.sleep(13);

            // 日志里面带有事件时间
            Long time = System.currentTimeMillis();
            String event = "flink(" + dateformat.format(time) + ")," + time;
            String event1 = event;
            cxt.collect(event);

            // 16s 输出一条数据
            TimeUnit.SECONDS.sleep(3);
            time = System.currentTimeMillis();
            cxt.collect("flink(" + dateformat.format(time) + ")," + time);

            // 本该 13s 输出的一条数据，延迟到 19s 的时候才输出
            TimeUnit.SECONDS.sleep(3);
            cxt.collect(event1);

            TimeUnit.SECONDS.sleep(30000000);
        }

        @Override
        public void cancel() {
        }
    }

    static class LongSumProcessFunction extends ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Integer>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Long>> allElements,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            int count = 0;
            for (Tuple2<String, Long> e : allElements) {
                count++;
            }
            out.collect(Tuple2.of(key, count));
        }
    }
}
