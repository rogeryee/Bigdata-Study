package com.yee.study.bigdata.flink.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Distinct 算子
 *
 * @author Roger.Yi
 */
public class DatasetTransformDistinct {

    public static void main(String[] args) throws Exception {
        // Env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Source
        List<String> data = new ArrayList<>();
        data.add("I jump");
        data.add("You jump");
        DataSource<String> ds = env.fromCollection(data);

        // flatMap + distinct
        DistinctOperator<String> resultDs = ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.toLowerCase().split("\\W+");
                for (String word : words) {
                    System.out.println("word: " + word);
                    out.collect(word);
                }
            }
        }).distinct();

        resultDs.print();
    }
}
