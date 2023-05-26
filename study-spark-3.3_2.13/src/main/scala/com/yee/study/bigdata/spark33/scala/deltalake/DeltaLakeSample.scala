package com.yee.study.bigdata.spark33.scala.deltalake

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

/**
 * Spark 读写 Hive 示例
 *
 * @author Roger.Yi
 */
object DeltaLakeSample {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("DeltaLakeSample")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    //    sample_1(spark) // 基于DataFrame读写数据（本地文件）
    //    sample_2(spark) // 基于DataFrame读写数据（HDFS）
    //    sample_3(spark) // 基于DataFrame读写数据（HIVE）
    //    sample_4(spark) // 基于 DeltaTable API 读写数据（HIVE）
    sample_5(spark) // 基于SparkSQL读写数据（包括CRUD操作）

    spark.close()
  }

  /**
   * 基于DataFrame读写数据（本地文件）
   *
   * @param spark
   */
  def sample_1(spark: SparkSession): Unit = {
    import spark.implicits._
    // Write
    val df = Seq(
      (1, "Roger", 30, "M", 2022),
      (2, "Andy", 40, "F", 2023)
    ).toDF("id", "name", "age", "gender", "dt")
    df.write
      .format("delta")
      .save("study-spark-3.3_2.13/data/employee_delta")

    // Read
    spark
      .read
      .format("delta")
      .load("study-spark-3.3_2.13/data/employee_delta")
      .show()
  }

  /**
   * 基于DataFrame读写数据（HDFS）
   *
   * @param spark
   */
  def sample_2(spark: SparkSession): Unit = {
    import spark.implicits._
    // Write
    val df = Seq(
      (1, "Roger", 30, "M", 2022),
      (2, "Andy", 40, "F", 2023)
    ).toDF("id", "name", "age", "gender", "dt")
    df.write
      .format("delta")
      .save("hdfs://localhost:9000/yish/employee_delta")

    // Read
    spark
      .read
      .format("delta")
      .load("hdfs://localhost:9000/yish/employee_delta")
      .show()
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
  def sample_3(spark: SparkSession): Unit = {
    import spark.implicits._
    // Write
    val df = Seq(
      (1, "Roger", 30, "M", 2022),
      (2, "Andy", 40, "F", 2023)
    ).toDF("id", "name", "age", "gender", "dt")
    df.write
      .format("delta")
      .saveAsTable("roger_tmp.employee_delta")

    spark.sql(
      """
        |select
        |*
        |from roger_tmp.employee_delta
        |""".stripMargin)
      .show
  }

  /**
   * 基于 DeltaTable API 读写数据（HIVE）
   *
   * @param spark
   */
  def sample_4(spark: SparkSession): Unit = {
    val tableName = "roger_tmp.employee_delta_v2"
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
  def sample_5(spark: SparkSession): Unit = {
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
}