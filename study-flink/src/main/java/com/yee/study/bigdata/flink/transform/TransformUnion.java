package com.yee.study.bigdata.flink.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * union 算子
 *
 * @author Roger.Yi
 */
public class TransformUnion {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> src1 = env.addSource(new MySource(1)).setParallelism(1);
        DataStreamSource<Long> src2 = env.addSource(new MySource(1000)).setParallelism(1);

        DataStream<Long> unionDs = src1.union(src2);

        unionDs.print().setParallelism(1);

        env.execute("TransformUnion");
    }

    /**
     * 自定义一个不带并行度的 Source
     */
    public static class MySource implements SourceFunction<Long> {

        public MySource(int start) {
            this.number = start;
        }

        private boolean isRunning = true;

        private long number = 1L;

        @Override
        public void run(SourceContext<Long> sourceContext) throws Exception {
            while (isRunning) {
                // 每隔 1s 钟发送一条数据
                sourceContext.collect(number++);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
