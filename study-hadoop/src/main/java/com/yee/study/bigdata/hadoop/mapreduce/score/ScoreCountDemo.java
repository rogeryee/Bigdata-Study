package com.yee.study.bigdata.hadoop.mapreduce.score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 统计课程得分
 * <p>
 * 1）创建一个txt文件(score.txt):
 * 19020090040 秦心芯 123 131 100 95 100
 * 19020090006 李磊 99 92 100 90 100
 * 19020090017 唐一建 90 99 100 89 95
 * 19020090031 曾丽丽 100 99 97 79 96
 * <p>
 * 2) 将该txt文件上传到HDFS上的input3目录
 * hadoop fs -mkdir /input3
 * hadoop fs -put /User/RogerYee/hufen.txt /input3
 * <p>
 * 3) 用 mvn clean install编译打包java-study-hadoop模块，会在target目录下生成 java-study-hadoop-1.0-SNAPSHOT.jar
 * <p>
 * 4) 进入target目录，执行如下命令启动map-reduce计算任务：
 * hadoop jar java-study-hadoop-1.0-SNAPSHOT.jar com.yee.study.hadoop.mapreduce.score.ScoreCountDemo
 * 注意：此处的输出目录每次都要指定一个不存在的。(也可以用 hadoop fs -rm -r /output 删除已经存在的输出目录)
 *
 * @author Roger.Yi
 */
public class ScoreCountDemo extends Configured implements Tool {

    //map和reduce
    public static class ScoreMapper extends Mapper<Text, ScoreWritable, Text, ScoreWritable> {
        @Override
        protected void map(Text key, ScoreWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class ScoreReducer extends Reducer<Text, ScoreWritable, Text, Text> {
        private Text text = new Text();

        @Override
        protected void reduce(Text key, Iterable<ScoreWritable> value, Context context) throws IOException, InterruptedException {
            float totalScore = 0.0f;
            float avgScore = 0.0f;
            for (ScoreWritable sw : value) {
                totalScore = sw.getChinese() + sw.getEnglish() + sw.getMath() + sw.getPhysics() + sw.getChemistry();
                avgScore = totalScore / 5;
            }
            text.set(totalScore + "\t" + avgScore);
            context.write(key, text);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //删除已经存在的输出目录
        Path mypath = new Path(args[1]);
        FileSystem hdfs = mypath.getFileSystem(conf);
        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }

        Job job = Job.getInstance(conf, "scorecount");
        job.setJarByClass(ScoreCountDemo.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);

        //如果是自定义的类型，需要进行设置
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ScoreWritable.class);

        //设置自定义的输入格式
        job.setInputFormatClass(ScoreInputFormat.class);
        job.waitForCompletion(true);
        return 0;
    }

    /**
     * hadoop jar java-study-hadoop-1.0-SNAPSHOT.jar com.yee.study.hadoop.mapreduce.score.ScoreCountDemo
     * (此处无需输入args)
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String[] args0 = {"hdfs://localhost:9000/input3", "hdfs://localhost:9000/output"};
        int res = ToolRunner.run(new ScoreCountDemo(), args0);
        System.exit(res);
    }
}
