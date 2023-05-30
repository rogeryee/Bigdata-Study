package com.yee.study.bigdata.spark33.scala

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.File

/**
 * 常用工具类
 *
 * @author Roger.Yi
 */
object Utils {

  /**
   * 删除本地目录
   *
   * @param path
   */
  def deleteLocalDirectory(path: String): Unit = {
    val file: File = new File(path)
    if (file.exists())
      FileUtils.deleteDirectory(file)
  }

  /**
   * 删除 hdfs 文件目录
   *
   * 需要将 hadoop的相应配置复制 resources 目录下
   */
  def deleteHdfsDirectory(spark: SparkSession, filePath: String): Unit = {
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val path = new Path(filePath)
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }

    hdfs.close()
  }
}
