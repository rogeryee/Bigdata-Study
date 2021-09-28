package com.yee.study.bigdata.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 简单的文字计数示例（基于map-reduce）
 * <p>
 * 1）创建一个txt文件(mf.txt)包含 男、女 2种字若干
 * 2) 将该txt文件上传到HDFS上的input目录
 * hadoop fs -mkdir /input
 * hadoop fs -put /User/RogerYee/mf.txt /input
 * 2) 用 mvn clean install编译打包java-study-hadoop模块，会在target目录下生成 java-study-hadoop-1.0-SNAPSHOT.jar
 * 3) 进入target目录，执行如下命令启动map-reduce计算任务：
 * hadoop jar java-study-hadoop-1.0-SNAPSHOT.jar com.yee.study.hadoop.mapreduce.wordcount.WordCountDemo /input /output1
 * 注意：此处的输出目录每次都要指定一个不存在的。(也可以用 hadoop fs -rm -r /output1 删除已经存在的输出目录)
 *
 * @author Roger.Yi
 */
public class WordCountDemo {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountDemo.class);
        job.setMapperClass(TokenizerMapper.class);

        // 添加自定义的分区
        job.setPartitionerClass(MyPartition.class);
        job.setNumReduceTasks(2);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
