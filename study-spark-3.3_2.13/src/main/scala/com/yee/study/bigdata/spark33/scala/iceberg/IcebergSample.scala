package com.yee.study.bigdata.spark33.scala.iceberg

import org.apache.spark.sql.SparkSession

/**
 * Spark 读写 Iceberg 示例
 *
 * @author Roger.Yi
 */
object IcebergSample {

  def main(args: Array[String]): Unit = {
    //    sample_1() // 基于 HDFS
    //    sample_2() // 基于 HDFS (基于默认的 spark_catalog)
    //    sample_3() // 基于 Hive （基本CRUD）
    //    sample_4() // 基于 Hive （复杂的操作）
    sample_5()
  }

  /**
   * 基于 HDFS
   *
   * @param spark
   */
  def sample_1(): Unit = {
    val spark: SparkSession = getSparkSession()
    try {
      // create table
      spark.sql(
        """
          |create table hadoop_local.iceberg.employee_iceberg
          |(
          | id int,
          | name string,
          | age int
          |) using iceberg
          |""".stripMargin)

      // insert data
      spark.sql(
        """
          |insert into hadoop_local.iceberg.employee_iceberg values
          | (1,"zs",18),
          | (2,"ls",19),
          | (3,"ww",20)
          |""".stripMargin)

      // query data
      spark.sql(
        """
          |select *
          |from hadoop_local.iceberg.employee_iceberg
          |where age >= 20
          |""".stripMargin)
        .show
    } finally {
      // Clean up
      spark.close()
    }
  }

  /**
   * 基于 HDFS (基于默认的 spark_catalog)
   *
   * @param spark
   */
  def sample_2(): Unit = {
    val spark: SparkSession = getSparkSessionDefault()
    try {
      // create table
      spark.sql(
        """
          |create table iceberg.employee_iceberg_v2
          |(
          | id int,
          | name string,
          | age int
          |) using iceberg
          |""".stripMargin)

      // insert data
      spark.sql(
        """
          |insert into iceberg.employee_iceberg_v2 values
          | (1,"zs",18),
          | (2,"ls",19),
          | (3,"ww",20)
          |""".stripMargin)

      // query data
      spark.sql(
        """
          |select *
          |from iceberg.employee_iceberg_v2
          |where age >= 20
          |""".stripMargin)
        .show
    } finally {
      // Clean up
      spark.close()
    }
  }

  /**
   * 基于 Hive （基本CRUD）
   *
   * @param spark
   */
  def sample_3(): Unit = {
    val spark: SparkSession = getSparkSessionWithHiveSupport()
    val tableName = "roger_tmp.employee_iceberg_v3"

    try {
      // create table
      spark.sql(
        s"""
           |create or table ${tableName}
           |(
           | id int,
           | name string,
           | age int
           |) using iceberg
           |""".stripMargin)

      // insert data
      spark.sql(
        s"""
           |insert into ${tableName} values
           | (1,'Roger',18),
           | (2,'Andy',19),
           | (3,'Joey',20)
           |""".stripMargin)

      // query data
      /**
       * +---+----+---+
       * | id|name|age|
       * +---+----+---+
       * |  3|Joey| 20|
       * +---+----+---+
       */
      spark.sql(
        s"""
           |select *
           |from ${tableName}
           |where age >= 20
           |""".stripMargin)
        .show
    } finally {
      // Clean up
      spark.sql(s"drop table ${tableName}")
      spark.close()
    }
  }

  /**
   * 基于 Hive （复杂的操作）
   *
   * @param spark
   */
  def sample_4(): Unit = {
    val spark: SparkSession = getSparkSessionWithHiveSupport()
    val tableName = "roger_tmp.employee_iceberg_v4"

    try {
      // 表如果不存在则创建表并初始化
      if (!spark.catalog.tableExists(tableName)) {
        // CREATE TABLE - 创建表
        spark.sql(
          s"""
             |create table ${tableName}
             |(
             | id int,
             | name string,
             | age int,
             | addr string,
             | dt bigint
             |) using iceberg
             |partitioned by (dt)
             |""".stripMargin)

        // INSERT INTO - 插入数据
        spark.sql(
          s"""
             |insert into ${tableName} values
             | (1,'Roger',18, 'Shanghai', 2021),
             | (2,'Andy',19, 'Beijing', 2021),
             | (3,'Joey',20, 'Shenzhen', 2022),
             | (4,'John',30, 'Shenzhen', 2023),
             | (5,'Amy',40, 'HongKong', 2023)
             |""".stripMargin)

        /**
         * +---+-----+---+--------+----+
         * | id| name|age|    addr|  dt|
         * +---+-----+---+--------+----+
         * |  1|Roger| 18|Shanghai|2021|
         * |  2| Andy| 19| Beijing|2021|
         * |  3| Joey| 20|Shenzhen|2022|
         * |  4| John| 30|Shenzhen|2023|
         * |  5|  Amy| 40|HongKong|2023|
         * +---+-----+---+--------+----+
         */
        spark.sql(s"select * from ${tableName}").show

        // DELETE FROM - 删除数据
        spark.sql(s"delete from ${tableName} where id = 3")

        /**
         * +---+-----+---+--------+----+
         * | id| name|age|    addr|  dt|
         * +---+-----+---+--------+----+
         * |  1|Roger| 18|Shanghai|2021|
         * |  2| Andy| 19| Beijing|2021|
         * |  4| John| 30|Shenzhen|2023|
         * |  5|  Amy| 40|HongKong|2023|
         * +---+-----+---+--------+----+
         */
        spark.sql(s"select * from ${tableName}").show

        // UPDATE -- 更新
        spark.sql(
          s"""
             |update ${tableName}
             |set addr = 'NewYork'
             |where id = 1
             |""".stripMargin)

        /**
         * +---+-----+---+--------+----+
         * | id| name|age|    addr|  dt|
         * +---+-----+---+--------+----+
         * |  1|Roger| 18| NewYork|2021|
         * |  2| Andy| 19| Beijing|2021|
         * |  4| John| 30|Shenzhen|2023|
         * |  5|  Amy| 40|HongKong|2023|
         * +---+-----+---+--------+----+
         */
        spark.sql(s"select * from ${tableName}").show

        // MERGE INTO - 合并
        /**
         * +---+-------+---+--------+----+
         * | id|   name|age|    addr|  dt|
         * +---+-------+---+--------+----+
         * |  1|  Roger| 18|  London|2021|
         * |  2|   Andy| 19| Beijing|2021|
         * |  6|Sparrow| 50|Shandong|2023|
         * |  4|   John| 30|Shenzhen|2023|
         * +---+-------+---+--------+----+
         */
        spark.sql(
          s"""
             |merge into ${tableName} t1
             |using (
             |   select 1 id, 'Roger' name, 18 age, 'London' addr, 2021 dt
             |   union all
             |   select 5 id, 'Amy' name, 40 age, 'Guangzhou' addr, 2023 dt
             |   union all
             |   select 6 id, 'Sparrow' name, 50 age, 'Shandong' addr, 2023 dt
             |) t2
             |   on t1.id = t2.id
             |when matched and t1.id = 1 then update set t1.addr = t2.addr
             |when matched and t1.id = 5 then delete
             |when not matched then insert *
             |""".stripMargin)

        spark.sql(s"select * from ${tableName}").show
      }
    } finally {
      // Clean up
      //      spark.sql(s"drop table ${tableName}")
      spark.close()
    }
  }

  /**
   * 基于 Hive （元数据操作）
   *
   * 注意：先执行 sample_4，且表未被删除
   *
   * @param spark
   */
  def sample_5(): Unit = {
    val spark: SparkSession = getSparkSessionWithHiveSupport()
    val tableName = "roger_tmp.employee_iceberg_v4"

    try {
      // 查询数据
      /**
       * +---+-------+---+--------+----+
       * | id|   name|age|    addr|  dt|
       * +---+-------+---+--------+----+
       * |  1|  Roger| 18|  London|2021|
       * |  2|   Andy| 19| Beijing|2021|
       * |  6|Sparrow| 50|Shandong|2023|
       * |  4|   John| 30|Shenzhen|2023|
       * +---+-------+---+--------+----+
       */
      spark.sql(
        s"""
           |select *
           |from ${tableName}
           |""".stripMargin)
        .show

      // 查询快照信息
      /**
       * +-----------------------+-------------------+-------------------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
       * |committed_at           |snapshot_id        |parent_id          |operation|manifest_list                                                                                                                                           |summary                                                                                                                                                                                                                                                                                                                                                                    |
       * +-----------------------+-------------------+-------------------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
       * |2023-05-31 16:05:46.204|2720086788001338884|null               |append   |hdfs://localhost:9000/user/hive/warehouse/roger_tmp.db/employee_iceberg_v4/metadata/snap-2720086788001338884-1-510efcf5-f485-41fb-9f53-ed8777355b08.avro|{spark.app.id -> local-1685520334909, added-data-files -> 3, added-records -> 5, added-files-size -> 4200, changed-partition-count -> 3, total-records -> 5, total-files-size -> 4200, total-data-files -> 3, total-delete-files -> 0, total-position-deletes -> 0, total-equality-deletes -> 0}                                                                           |
       * |2023-05-31 16:05:49.167|4161688696542340816|2720086788001338884|delete   |hdfs://localhost:9000/user/hive/warehouse/roger_tmp.db/employee_iceberg_v4/metadata/snap-4161688696542340816-1-356df761-3527-4de1-93ac-0ee4befbf1c2.avro|{spark.app.id -> local-1685520334909, deleted-data-files -> 1, deleted-records -> 1, removed-files-size -> 1374, changed-partition-count -> 1, total-records -> 4, total-files-size -> 2826, total-data-files -> 2, total-delete-files -> 0, total-position-deletes -> 0, total-equality-deletes -> 0}                                                                     |
       * |2023-05-31 16:05:51.032|7354858758510673615|4161688696542340816|overwrite|hdfs://localhost:9000/user/hive/warehouse/roger_tmp.db/employee_iceberg_v4/metadata/snap-7354858758510673615-1-80066818-a35c-4e19-8be2-0e3955a5217b.avro|{spark.app.id -> local-1685520334909, added-data-files -> 1, deleted-data-files -> 1, added-records -> 2, deleted-records -> 2, added-files-size -> 1449, removed-files-size -> 1416, changed-partition-count -> 1, total-records -> 4, total-files-size -> 2859, total-data-files -> 2, total-delete-files -> 0, total-position-deletes -> 0, total-equality-deletes -> 0}|
       * |2023-05-31 16:05:54.575|3998410543509778121|7354858758510673615|overwrite|hdfs://localhost:9000/user/hive/warehouse/roger_tmp.db/employee_iceberg_v4/metadata/snap-3998410543509778121-1-84ebf867-b455-40ad-8680-2996b2a30b5a.avro|{spark.app.id -> local-1685520334909, added-data-files -> 2, deleted-data-files -> 2, added-records -> 4, deleted-records -> 4, added-files-size -> 2906, removed-files-size -> 2859, changed-partition-count -> 2, total-records -> 4, total-files-size -> 2906, total-data-files -> 2, total-delete-files -> 0, total-position-deletes -> 0, total-equality-deletes -> 0}|
       * +-----------------------+-------------------+-------------------+---------+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
       */
      spark.sql(
        s"""
           |select *
           |from ${tableName}.snapshots
           |""".stripMargin)
        .show(false)

      // 查询历史信息
      /**
       * +-----------------------+-------------------+-------------------+-------------------+
       * |made_current_at        |snapshot_id        |parent_id          |is_current_ancestor|
       * +-----------------------+-------------------+-------------------+-------------------+
       * |2023-05-31 16:05:46.204|2720086788001338884|null               |true               |
       * |2023-05-31 16:05:49.167|4161688696542340816|2720086788001338884|true               |
       * |2023-05-31 16:05:51.032|7354858758510673615|4161688696542340816|true               |
       * |2023-05-31 16:05:54.575|3998410543509778121|7354858758510673615|true               |
       * +-----------------------+-------------------+-------------------+-------------------+
       */
      spark.sql(
        s"""
           |select *
           |from ${tableName}.history
           |""".stripMargin)
        .show(false)

      // 查询文件
      /**
       * +-------+----------------------------------------------------------------------------------------------------------------------------------------------------+-----------+-------+---------+------------+------------------+---------------------------------------------+----------------------------------------+----------------------------------------+----------------+----------------------------------------------------------------+-------------------------------------------------------------------+------------+-------------+------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------+
       * |content|file_path                                                                                                                                           |file_format|spec_id|partition|record_count|file_size_in_bytes|column_sizes                                 |value_counts                            |null_value_counts                       |nan_value_counts|lower_bounds                                                    |upper_bounds                                                       |key_metadata|split_offsets|equality_ids|sort_order_id|readable_metrics                                                                                                                                       |
       * +-------+----------------------------------------------------------------------------------------------------------------------------------------------------+-----------+-------+---------+------------+------------------+---------------------------------------------+----------------------------------------+----------------------------------------+----------------+----------------------------------------------------------------+-------------------------------------------------------------------+------------+-------------+------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------+
       * |0      |hdfs://localhost:9000/user/hive/warehouse/roger_tmp.db/employee_iceberg_v4/data/dt=2021/00023-418-5fc85cea-80f4-4ed4-b8c8-d118376b881b-00001.parquet|PARQUET    |0      |{2021}   |2           |1447              |{1 -> 54, 2 -> 63, 3 -> 54, 4 -> 67, 5 -> 93}|{1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 2}|{1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0, 5 -> 0}|{}              |{1 ->    , 2 -> Andy, 3 ->    , 4 -> Beijing, 5 -> �\a      } |{1 ->    , 2 -> Roger, 3 ->    , 4 -> London, 5 -> �\a      }    |null        |[4]          |null        |0            |{{67, 2, 0, null, Beijing, London}, {54, 2, 0, null, 18, 19}, {93, 2, 0, null, 2021, 2021}, {54, 2, 0, null, 1, 2}, {63, 2, 0, null, Andy, Roger}}     |
       * |0      |hdfs://localhost:9000/user/hive/warehouse/roger_tmp.db/employee_iceberg_v4/data/dt=2023/00152-419-5fc85cea-80f4-4ed4-b8c8-d118376b881b-00001.parquet|PARQUET    |0      |{2023}   |2           |1459              |{1 -> 53, 2 -> 65, 3 -> 53, 4 -> 66, 5 -> 94}|{1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 2}|{1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0, 5 -> 0}|{}              |{1 ->    , 2 -> John, 3 ->    , 4 -> Shandong, 5 -> �\a      }|{1 ->    , 2 -> Sparrow, 3 -> 2   , 4 -> Shenzhen, 5 -> �\a      }|null        |[4]          |null        |0            |{{66, 2, 0, null, Shandong, Shenzhen}, {53, 2, 0, null, 30, 50}, {94, 2, 0, null, 2023, 2023}, {53, 2, 0, null, 4, 6}, {65, 2, 0, null, John, Sparrow}}|
       * +-------+----------------------------------------------------------------------------------------------------------------------------------------------------+-----------+-------+---------+------------+------------------+---------------------------------------------+----------------------------------------+----------------------------------------+----------------+----------------------------------------------------------------+-------------------------------------------------------------------+------------+-------------+------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------------+
       */
      spark.sql(
        s"""
           |select *
           |from ${tableName}.files
           |""".stripMargin)
        .show(false)

      // Time Travel (基于 timestamp，对应 timestamp-ms <= as-of-timestamp 对应的 snapshot-id)
      /**
       * +---+-----+---+--------+----+
       * |id |name |age|addr    |dt  |
       * +---+-----+---+--------+----+
       * |1  |Roger|18 |Shanghai|2021|
       * |2  |Andy |19 |Beijing |2021|
       * |4  |John |30 |Shenzhen|2023|
       * |5  |Amy  |40 |HongKong|2023|
       * +---+-----+---+--------+----+
       */
      spark.sql(
        s"""
           |select *
           |from ${tableName} timestamp as of '2023-05-31 16:05:49.167'
           |""".stripMargin)
        .show(false)

      // Time Travel (基于 snapshot_id)
      /**
       * +---+-----+---+--------+----+
       * |id |name |age|addr    |dt  |
       * +---+-----+---+--------+----+
       * |4  |John |30 |Shenzhen|2023|
       * |5  |Amy  |40 |HongKong|2023|
       * |1  |Roger|18 |NewYork |2021|
       * |2  |Andy |19 |Beijing |2021|
       * +---+-----+---+--------+----+
       */
      spark.sql(
        s"""
           |select *
           |from ${tableName} version as of 7354858758510673615
           |""".stripMargin)
        .show(false)

    } finally {
      // Clean up
      //      spark.sql(s"drop table ${tableName}")
      spark.close()
    }
  }

  /**
   * 获取 SparkSession（基于HDFS）
   *
   * @return
   */
  def getSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("DeltaLakeSample")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.hadoop_local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_local.type", "hadoop")
      .config("spark.sql.catalog.hadoop_local.warehouse", "hdfs://localhost:9000/yish")
      .getOrCreate()
  }

  /**
   * 获取 SparkSession（基于HDFS）
   *
   * @return
   */
  def getSparkSessionDefault(): SparkSession = {
    SparkSession
      .builder()
      .appName("DeltaLakeSample")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.catalog.spark_catalog.warehouse", "hdfs://localhost:9000/yish")
      .getOrCreate()
  }

  /**
   * 获取 SparkSession（基于Hive）
   *
   * @return
   */
  def getSparkSessionWithHiveSupport(): SparkSession = {
    SparkSession
      .builder()
      .appName("DeltaLakeSample")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.catalog.spark_catalog.uri", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()
  }
}