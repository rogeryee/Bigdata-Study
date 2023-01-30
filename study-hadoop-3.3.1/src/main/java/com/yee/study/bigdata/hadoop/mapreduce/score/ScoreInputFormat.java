package com.yee.study.bigdata.hadoop.mapreduce.score;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 课程得分输入格式
 *
 * @author Roger.Yi
 */
public class ScoreInputFormat extends FileInputFormat<Text, ScoreWritable> {

    //需要注意的是：
    /*
     * 对于一个数据输入格式，都需要一个对应的RecordReader
     * 重写createRecordReader()方法，其实也就是重写其返回的对象
     * 这里就是自定义的ScoreRecordReader类，该类需要继承RecordReader，实现数据的读取
     * */
    @Override
    public RecordReader<Text, ScoreWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new ScoreRecordReader();
    }
}
