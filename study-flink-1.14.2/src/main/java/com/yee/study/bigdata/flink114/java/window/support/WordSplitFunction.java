package com.yee.study.bigdata.flink114.java.window.support;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 基于逗号分隔字符串的函数
 *
 * @author Roger.Yi
 */
public class WordSplitFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] words = value.split(",");
        Arrays.stream(words).map(word -> Tuple2.of(word, 1)).forEach(out::collect);
    }
}
