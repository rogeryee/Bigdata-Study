package com.yee.study.bigdata.hadoop.mapreduce.score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * 得分记录读取器
 *
 * @author Roger.Yi
 */
public class ScoreRecordReader extends RecordReader<Text, ScoreWritable> {

    public LineReader in;                //行读取器
    public Text lineKey;                //自定义key类型
    public ScoreWritable linevalue;    //自定义的value类型
    public Text line;                    //行数据

    //初始化方法，只执行一次
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fsplit = (FileSplit) split;
        Configuration conf = context.getConfiguration();
        Path file = fsplit.getPath();
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream filein = fs.open(file);
        in = new LineReader(filein, conf);
        line = new Text();
        lineKey = new Text();
        linevalue = new ScoreWritable();
    }

    //读取每一行数据的时候，都会执行该方法
    //我们只需要根据自己的需求，重点编写该方法即可，其他的方法比较固定，仿照就好
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        int linesize = in.readLine(line);
        if (linesize == 0) {
            return false;
        }
        String[] pieces = line.toString().split("\\s+");
        if (pieces.length != 7) {
            throw new IOException("无效的数据");
        }
        //将学生的每门成绩转换为float类型
        float a = 0, b = 0, c = 0, d = 0, e = 0;
        try {
            a = Float.parseFloat(pieces[2].trim());
            b = Float.parseFloat(pieces[3].trim());
            c = Float.parseFloat(pieces[4].trim());
            d = Float.parseFloat(pieces[5].trim());
            e = Float.parseFloat(pieces[6].trim());
        } catch (NumberFormatException nfe) {
            nfe.printStackTrace();
        }
        lineKey.set(pieces[0] + "\t" + pieces[1]);    //完成自定义的key数据
        linevalue.set(a, b, c, d, e);                //封装自定义的value数据
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return lineKey;
    }

    @Override
    public ScoreWritable getCurrentValue() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return linevalue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}
