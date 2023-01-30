package com.yee.study.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS 操作示例
 * @author Roger.Yi
 */
public class HdfsDemo {

    private static final Logger logger = LoggerFactory.getLogger(HdfsDemo.class);

    /**
     * $HADOOP_HOME/etc/hadoop/core.xml中HDFS的配置
     */
    private static String hdfsServerName = "fs.defaultFS";

    /**
     * $HADOOP_HOME/etc/hadoop/core.xml中HDFS的配置
     */
    private static String hdfsServerHost = "hdfs://localhost:9000";

    public static void main(String[] args) throws Exception {
        
        // 创建Configuration对象
        Configuration conf = new Configuration();
        conf.set(hdfsServerName, hdfsServerHost);

        // 创建FileSystem对象
        FileSystem fs = FileSystem.get(conf);

        // 打开文件
//        openFile(fs);

        // 下载文件
//        download(fs);

        // 上传文件
//        upload(fs);

        // 删除文件
//        delete(fs);

        listDir(fs);
    }

    /**
     * 打开文件
     * @param fs
     * @throws Exception
     */
    private static void openFile(FileSystem fs) throws Exception {
        String filePath = "/tfiles/a.txt";
        FSDataInputStream is = fs.open(new Path(filePath));

        byte[] buff = new byte[1024];
        int length = 0;
        while ((length = is.read(buff)) != -1) {
            logger.info(new String(buff, 0, length));
        }
    }

    /**
     * 下载文件
     * @param fs
     * @throws Exception
     */
    private static void download(FileSystem fs) throws Exception {
        String filePath = "/tfiles/a.txt";
        String localPath = "/Users/RogerYee/";
        fs.copyToLocalFile(new Path(filePath),new Path(localPath));
    }

    /**
     * 上传文件
     * @param fs
     * @throws Exception
     */
    private static void upload(FileSystem fs) throws Exception {
        String filePath = "/tfiles";
        String localPath = "/Users/RogerYee/c.txt";
        fs.copyFromLocalFile(new Path(localPath),new Path(filePath));
    }

    /**
     * 删除文件
     * @param fs
     * @throws Exception
     */
    private static void delete(FileSystem fs) throws Exception {
        String filePath = "/tfiles/c.txt";
        fs.delete(new Path(filePath));
    }

    /**
     * 删除文件
     * @param fs
     * @throws Exception
     */
    private static void listDir(FileSystem fs) throws Exception {
        String path = "/tfiles";
        FileStatus[] statuses = fs.listStatus(new Path(path));
        for(FileStatus status : statuses) {
            logger.info("path:" + status.getPath());
        }
    }
}
