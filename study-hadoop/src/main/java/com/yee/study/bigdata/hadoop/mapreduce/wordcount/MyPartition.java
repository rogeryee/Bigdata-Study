package com.yee.study.bigdata.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义的Partitioner
 *
 * @author Roger.Yi
 */
public class MyPartition extends Partitioner<Object, IntWritable> {

    private static final String MALE = "男";

    private static final String FEMALE = "女";

    /**
     * 本例根据男、女进行不同的分区
     *
     * @param key
     * @param value
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(Object key, IntWritable value, int numPartitions) {
        final String ke = key.toString();

        if (MALE.equals(ke)) {
            return 0;
        } else if (FEMALE.equals(ke)) {
            return 1;//% numPartitions;
        }

        return 2;
    }
}
