package com.yee.study.bigdata.spark33.scala.deltalake

import com.yee.study.bigdata.spark33.scala.Utils
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

/**
 * Spark 读写 DeltaLake 示例
 *
 * @author Roger.Yi
 */
object DeltaLakeSample {

  def main(args: Array[String]): Unit = {
    //    sample_1() // 基于DataFrame读写数据（本地文件）
    //    sample_2() // 基于DataFrame读写数据（HDFS）
    //    sample_3() // 基于DataFrame读写数据（HIVE）
    //    sample_4() // 基于 DeltaTable API 读写数据（HIVE）
    sample_5() // 基于SparkSQL读写数据（包括CRUD操作）
  }

  /**
   * 基于DataFrame读写数据（本地文件）
   *
   * @param spark
   */
  def sample_1(): Unit = {
    val spark: SparkSession = getSparkSession
    import spark.implicits._

    val filePath = "study-spark-3.3_2.13/data/employee_delta"
    try {
      // Write data
      val df = Seq(
        (1, "Roger", 30, "M", 2022),
        (2, "Andy", 40, "F", 2023)
      ).toDF("id", "name", "age", "gender", "dt")
      df.write
        .mode("overwrite")
        .format("delta")
        .save(filePath)

      // Read data
      spark
        .read
        .format("delta")
        .load(filePath)
        .show()
    } finally {
      // Clean up
      Utils.deleteLocalDirectory(filePath)
      spark.close()
    }
  }

  /**
   * 基于DataFrame读写数据（HDFS）
   *
   * 删除原有目录：hadoop fs -rm -r /yish/employee_delta_hdfs
   *
   * @param spark
   */
  def sample_2(): Unit = {
    val spark: SparkSession = getSparkSession()
    import spark.implicits._

    val path = "hdfs://localhost:9000/yish/employee_delta_hdfs"
    try {
      // Write data
      val df = Seq(
        (1, "Roger", 30, "M", 2022),
        (2, "Andy", 40, "F", 2023)
      ).toDF("id", "name", "age", "gender", "dt")

      df.write
        .mode("overwrite")
        .format("delta")
        .save(path)

      // Read data
      spark
        .read
        .format("delta")
        .load(path)
        .show()
    } finally {
      // Clean up
      Utils.deleteHdfsDirectory(spark, path)
      spark.close()
    }
  }

  /**
   * 基于DataFrame读写数据（HIVE）
   *
   * SparkSession 配置：
   * .config("hive.metastore.uris", "thrift://localhost:9083")
   * .enableHiveSupport()
   *
   * @param spark
   */
  def sample_3(): Unit = {
    val spark: SparkSession = getSparkSessionWithHiveSupport()
    import spark.implicits._

    val tableName = "roger_tmp.employee_delta"
    if (spark.catalog.tableExists(tableName)) {
      spark.sql(s"drop table ${tableName}")
    }

    // Write
    val df = Seq(
      (1, "Roger", 30, "M", 2022),
      (2, "Andy", 40, "F", 2023)
    ).toDF("id", "name", "age", "gender", "dt")
    df.write
      .format("delta")
      .saveAsTable(tableName)

    spark.sql(
      s"""
         |select
         |*
         |from ${tableName}
         |""".stripMargin)
      .show
  }

  /**
   * 基于 DeltaTable API 读写数据（HIVE）
   *
   * hadoop fs -rm -r /user/hive/warehouse/roger_tmp.db/employee_delta_v2
   *
   * @param spark
   */
  def sample_4(): Unit = {
    val spark: SparkSession = getSparkSessionWithHiveSupport()
    val tableName = "roger_tmp.employee_delta_v2"
    if (spark.catalog.tableExists(tableName)) {
      spark.sql(s"drop table ${tableName}")
    }

    DeltaTable.createOrReplace(spark)
      .tableName(tableName)
      .addColumn("id", "INT")
      .addColumn("name", "STRING")
      .addColumn("gender", "STRING")
      .addColumn("login_time", "TIMESTAMP")
      .addColumn(DeltaTable.columnBuilder("year")
        .dataType(IntegerType)
        .generatedAlwaysAs("YEAR(login_time)")
        .build())
      .addColumn(DeltaTable.columnBuilder("month")
        .dataType(IntegerType)
        .generatedAlwaysAs("MONTH(login_time)")
        .build())
      .addColumn(DeltaTable.columnBuilder("day")
        .dataType(IntegerType)
        .generatedAlwaysAs("DAY(login_time)")
        .build())
      .partitionedBy("year", "month", "day")
      .execute()

    spark.sql(
      s"""
         |insert into ${tableName}
         |select 1 id, 'Roger' name, 'M' gender, '2023-05-26 10:21:48' login_time, 2023 year, 5 month, 26 day
         |""".stripMargin)
      .show

    /**
     * +---+-----+------+-------------------+----+-----+---+
     * | id| name|gender|         login_time|year|month|day|
     * +---+-----+------+-------------------+----+-----+---+
     * |  1|Roger|     M|2023-05-26 10:21:48|2023|    5| 26|
     * +---+-----+------+-------------------+----+-----+---+
     */
    spark.sql(
      s"""
         |select
         |*
         |from ${tableName}
         |""".stripMargin)
      .show
  }

  /**
   * 基于SparkSQL读写数据
   *
   * @param spark
   */
  def sample_5(): Unit = {
    val spark: SparkSession = getSparkSessionWithHiveSupport()
    val tableName = "roger_tmp.employee_delta_v3"
    if (spark.catalog.tableExists(tableName)) {
      spark.sql(s"drop table ${tableName}")
    }

    // create table
    spark.sql(
      s"""
         |create table ${tableName}
         |using delta
         |as
         |select 1 id, 'Roger' name, 30 age union all
         |select 2 id, 'Andy' name, 40 age union all
         |select 3 id, 'Joey' name, 50 age
         |""".stripMargin)

    /**
     * +---+-----+---+
     * | id| name|age|
     * +---+-----+---+
     * |  1|Roger| 30|
     * |  3| Joey| 50|
     * |  2| Andy| 40|
     * +---+-----+---+
     */
    spark.sql(
      s"""
         |select *
         |from ${tableName}
         |""".stripMargin)
      .show

    // overwrite table
    spark.sql(
      s"""
         |insert overwrite ${tableName}
         |select 1 id, 'Roger' name, 30 age union all
         |select 3 id, 'Joey' name, 50 age union all
         |select 4 id, 'John' name, 60 age union all
         |select 5 id, 'Amy' name, 70 age
         |""".stripMargin)

    /**
     * +---+-----+---+
     * | id| name|age|
     * +---+-----+---+
     * |  1|Roger| 30|
     * |  3| Joey| 50|
     * |  4| John| 60|
     * |  5|  Amy| 70|
     * +---+-----+---+
     */
    spark.sql(
      s"""
         |select *
         |from ${tableName}
         |""".stripMargin)
      .show

    // update data
    spark.sql(
      s"""
         |update ${tableName}
         |set age = age + 1
         |where id = 3
         |""".stripMargin)

    spark.sql(
      s"""
         |delete from ${tableName}
         |where id % 2 = 0
         |""".stripMargin)

    /**
     * +---+-----+---+
     * | id| name|age|
     * +---+-----+---+
     * |  1|Roger| 30|
     * |  3| Joey| 51|
     * |  5|  Amy| 70|
     * +---+-----+---+
     */
    spark.sql(
      s"""
         |select *
         |from ${tableName}
         |""".stripMargin)
      .show
  }

  /**
   * 获取 SparkSession
   *
   * @return
   */
  def getSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("DeltaLakeSample")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  /**
   * 获取 SparkSession（支持Hive）
   *
   * @return
   */
  def getSparkSessionWithHiveSupport(): SparkSession = {
    SparkSession
      .builder()
      .appName("DeltaLakeSample")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()
  }
}