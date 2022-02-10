package com.yee.study.bigdata.flink.source.userdefine;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * 自定义的带并行度的数据源
 * <p>
 * 基于给定的起始值和间隔时间，输出一系列的自增自然数
 *
 * @author Roger.Yi
 */
public class LongSourceWithParallel implements ParallelSourceFunction<Long> {

    private boolean isRunning = true;
    private long interval = 1000;
    private long number = 1L;

    public LongSourceWithParallel(int start, int interval) {
        this.number = start;
        this.interval = interval;
    }

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning) {
            // 每隔 interval 发送一条数据
            sourceContext.collect(number++);
            Thread.sleep(this.interval);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
