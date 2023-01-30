package com.yee.study.bigdata.hadoop.mapreduce.hufen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 统计互相关联（互粉）
 * <p>
 * 1）创建一个txt文件(hufen.txt):
 * A:B,C,D,F,E,O
 * B:B,C,E,K
 * C:F,A,D,I
 * 2) 将该txt文件上传到HDFS上的input1目录
 * hadoop fs -mkdir /input2
 * hadoop fs -put /User/RogerYee/hufen.txt /input2
 * 2) 用 mvn clean install编译打包java-study-hadoop模块，会在target目录下生成 java-study-hadoop-1.0-SNAPSHOT.jar
 * 3) 进入target目录，执行如下命令启动map-reduce计算任务：
 * hadoop jar java-study-hadoop-1.0-SNAPSHOT.jar com.yee.study.hadoop.mapreduce.hufen.HufenDemo /input2 /output
 * 注意：此处的输出目录每次都要指定一个不存在的。(也可以用 hadoop fs -rm -r /output 删除已经存在的输出目录)
 *
 * @author Roger.Yi
 */
public class HufenDemo {

    public static class Map extends Mapper<LongWritable,Text,Text,NullWritable> {

        private Text keyout = new Text();
        private Text valueout = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] rdLine = value.toString().trim().split(":");
            if(rdLine != null && rdLine.length == 2) {
                String username = rdLine[0];
                String[] fans = rdLine[1].split(",");
                for (String fan : fans) {

                    String hufenzu;
                    if (username.compareTo(fan) < 0) {
                        hufenzu = username + "-" + fan; // A-B
                    } else {
                        hufenzu = fan + "-" + username; // B-A => A-B
                    }

                    keyout.set(hufenzu);
                    context.write(keyout, NullWritable.get());
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {
        Text keyout = new Text();
        Text valueout = new Text();

        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (NullWritable text : values) {
                count++;
            }
            if (count == 2) {
                context.write(key, NullWritable.get());
            }
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
        job.setJarByClass(HufenDemo.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
