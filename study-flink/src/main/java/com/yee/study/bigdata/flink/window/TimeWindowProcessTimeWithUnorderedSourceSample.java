package com.yee.study.bigdata.flink.window;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * 使用 ProcessTime 作为处理时间
 *
 * 需求：每隔5秒计算最近10秒的单词次数
 *
 * 数据源（乱序输出）
 * 17:33:13 输出 1 条数据 event1（ProcessTime=17:33:13）
 * 17:33:16 输出 1 条数据 event3（ProcessTime=17:33:16）
 * 17:33:19 输出 1 条数据 event2（ProcessTime=17:33:19）
 *
 * event2 本该 17:33:13 输出，延迟到 17:33:19 输出
 *
 * 窗口日志：
 * 17:33:05  window [17:32:55 - 17:33:05] 窗口无数据，不触发计算
 * 17:33:10  window [17:33:00 - 17:33:10] 窗口无数据，不触发计算
 * 17:33:15  window [17:33:05 - 17:33:15] 包含 1 条数据（event1），输出 (flink, 1)
 * 17:33:20  window [17:33:10 - 17:33:20] 包含 3 条数据（event1、event2、event3），输出 (flink, 3)
 * 17:33:25  window [17:33:15 - 17:33:25] 包含 2 条数据（event2、event3），输出 (flink, 2)
 * 17:33:30  window [17:33:20 - 17:33:30] 窗口无数据，不触发计算
 *
 * 其中event2 本该只应该出现在 window [17:33:10 - 17:33:20]，而用 ProcessTime 则无法避免乱序数据造成的窗口数据的异常
 *
 * @author Roger.Yi
 */
public class TimeWindowProcessTimeWithUnorderedSourceSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source
        DataStreamSource<String> source = env.addSource(new UnOrderedSource());

        // Operator
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds = source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = value.split(",");
                        Arrays.stream(words).map(word -> Tuple2.of(word, 1)).forEach(out::collect);
                    }
                })
                .keyBy(tuple -> tuple.f0)
                // 每隔 5s 计算过去 10s内 数据的结果。用的是 ProcessingTime
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time
                        .seconds(5)))
                .process(new SumProcessFunction());

        // Sink
        ds.print().setParallelism(1);

        // run
        env.execute("TimeWindowWithOrderedSourceSample");
    }

    /**
     * 注释： 自定义的顺序 source
     * 1、在第 13s 的时候，输出两条数据
     * 2、在第 16s 的时候，输出一条数据
     */
    public static class UnOrderedSource implements SourceFunction<String> {

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
            String log = "flink";
            cxt.collect(log);

            // 16s 输出一条数据
            TimeUnit.SECONDS.sleep(3);
            cxt.collect("flink");

            // 本该 13s 输出的一条数据延迟到 19s 的时候才输出
            TimeUnit.SECONDS.sleep(3);
            cxt.collect(log);

            TimeUnit.SECONDS.sleep(30000000);
        }

        @Override
        public void cancel() {
        }
    }
}
