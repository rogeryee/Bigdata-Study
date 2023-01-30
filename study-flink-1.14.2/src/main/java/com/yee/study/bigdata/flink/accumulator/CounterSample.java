package com.yee.study.bigdata.flink.accumulator;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * Flink 累加器 示例
 *
 * @author Roger.Yi
 */
public class CounterSample {

    public static void main(String[] args) throws Exception {
        // ENV
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Dataset
        // total = count(b) + count(c) + count(d)
        // 字母a 不参与统计
        DataSource<String> data = env.fromElements("a", "b", "c", "d", "a", "b", "c", "d", "a", "b", "c", "d");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            // 创建累加器
            private IntCounter counter = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                // 注册累加器
                getRuntimeContext().addAccumulator("mycounter", this.counter);
            }

            @Override
            public String map(String value) throws Exception {
                if(value.equals("a") == false) {
                    this.counter.add(1);
                }
                return value;
            }
        }).setParallelism(4);

        result.print();

        JobExecutionResult jobResult = env.execute("counter");
        int count = jobResult.getAccumulatorResult("mycounter");
        System.out.println("counter = " + count);
    }
}
