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
 * 自定义数据源：顺序输出
 * 第13秒输出2条数据；
 * 第16秒输出 1 条数据；
 * <p>
 * 需求：每隔5秒计算最近10秒的单词次数
 * <p>
 * 数据源：
 * 17:33:13 输出2条数据
 * 17:33:16 输出1条数据
 * <p>
 * 处理端：
 * 17:33:00  不触发 window 计算
 * 17:33:05  不触发 window 计算
 * 17:33:10  不触发 window 计算
 * 17:33:15  [17:33:05 - 17:33:15] 输出 (flink, 2)
 * 17:33:20  [17:33:10 - 17:33:20] 输出 (flink, 3)
 * 17:33:25  [17:33:15 - 17:33:25] 输出 (flink, 1)
 * 17:33:30  不触发 window 计算
 *
 * @author Roger.Yi
 */
public class TimeWindowWithOrderedSourceSample {

    public static void main(String[] args) throws Exception {
        // Env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source
        DataStreamSource<String> source = env.addSource(new OrderedSource());

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
    public static class OrderedSource implements SourceFunction<String> {

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

            // 第 13s 输出两条数据
            TimeUnit.SECONDS.sleep(13);
            cxt.collect("flink");
            cxt.collect("flink");

            // TODO_MA 马中华 注释： 16s 输出一条数据  :  20:53:26
            TimeUnit.SECONDS.sleep(3);
            cxt.collect("flink");

            TimeUnit.SECONDS.sleep(30000000);
        }

        @Override
        public void cancel() {
        }
    }
}
