package com.yee.study.bigdata.flink114.java.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Instant;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.concat;

/**
 * @author Roger.Yi
 */
public class TableApiSample {

    public static void main(String[] args) throws Exception {
        // Stream env
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // Table env
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(sEnv);

        // Scan, Projection, Filter 示例
//        scanSamples(sEnv, tabEnv);

        // Column Operation 示例
//        columnOperationSamples(sEnv, tabEnv);

        // Aggregation 示例
        aggregationSamples(sEnv, tabEnv);
    }

    /**
     * Scan, Projection, Filter 示例
     *
     * @param sEnv
     * @param tabEnv
     * @throws Exception
     */
    public static void scanSamples(StreamExecutionEnvironment sEnv, StreamTableEnvironment tabEnv) throws Exception {
        // DataStream
        DataStream<Order> dataStream = sEnv.fromElements(
                new Order("Roger", "China", "Shanghai", Instant.ofEpochMilli(1000)),
                new Order("Andy", "China", "Shanghai", Instant.ofEpochMilli(1000)),
                new Order("John", "USA", "NY", Instant.ofEpochMilli(1000)),
                new Order("Eric", "USA", "LA", Instant.ofEpochMilli(1000)),
                new Order("Rooney", "UK", "MAN", Instant.ofEpochMilli(1000)));

        tabEnv.createTemporaryView(
                "Orders",
                dataStream);
        Table orders = tabEnv.from("Orders"); // schema (a, b, c, rowtime)

        /**
         * 1) Overview & Samples
         * +----+--------------------------------+----------------------+
         * | op |                              a |                  cnt |
         * +----+--------------------------------+----------------------+
         * | +I |                          Roger |                    1 |
         * | +I |                         Rooney |                    1 |
         * | +I |                           Andy |                    1 |
         * | +I |                           John |                    1 |
         * | +I |                           Eric |                    1 |
         * +----+--------------------------------+----------------------+
         */
        Table counts = orders
                .groupBy($("a"))
                .select($("a"), $("b").count().as("cnt"));
        counts.execute().print();

        /**
         * 2) Select
         * +----+--------------------------------+--------------------------------+
         * | op |                              a |                              d |
         * +----+--------------------------------+--------------------------------+
         * | +I |                          Roger |                       Shanghai |
         * | +I |                           Andy |                       Shanghai |
         * | +I |                           John |                             NY |
         * | +I |                           Eric |                             LA |
         * | +I |                         Rooney |                            MAN |
         * +----+--------------------------------+--------------------------------+
         */
        Table result = orders.select($("a"), $("c").as("d")); // orders.select($("*"));
        result.execute().print();

        /**
         * 3) As
         * +----+----------+----------+-------------+-------------------------------+
         * | op |        x |        y |           z |                             t |
         * +----+----------+----------+-------------+-------------------------------+
         * | +I |    Roger |    China |    Shanghai | 1970-01-01 08:00:01.000000000 |
         * | +I |     Andy |    China |    Shanghai | 1970-01-01 08:00:01.000000000 |
         * | +I |     John |      USA |          NY | 1970-01-01 08:00:01.000000000 |
         * | +I |     Eric |      USA |          LA | 1970-01-01 08:00:01.000000000 |
         * | +I |   Rooney |       UK |         MAN | 1970-01-01 08:00:01.000000000 |
         * +----+----------+----------+-------------+-------------------------------+
         */
        result = orders.as("x, y, z, t");
        result.execute().print();

        /**
         * 4) Where/Filter
         *
         * +----+--------+--------+-----------+-------------------------------+
         * | op |      a |      b |         c |                       rowtime |
         * +----+--------+--------+-----------+-------------------------------+
         * | +I |  Roger |  China |  Shanghai | 1970-01-01 08:00:01.000000000 |
         * | +I |   Andy |  China |  Shanghai | 1970-01-01 08:00:01.000000000 |
         * +----+--------+--------+-----------+-------------------------------+
         */
        result = orders.filter($("b").isEqual("China"));
        result.execute().print();
    }

    /**
     * Column Operator 示例
     *
     * @param sEnv
     * @param tabEnv
     * @throws Exception
     */
    public static void columnOperationSamples(StreamExecutionEnvironment sEnv, StreamTableEnvironment tabEnv) throws Exception {
        // DataStream
        DataStream<Order> dataStream = sEnv.fromElements(
                new Order("Roger", "China", "Shanghai", Instant.ofEpochMilli(1000)),
                new Order("Andy", "China", "Shanghai", Instant.ofEpochMilli(1000)),
                new Order("John", "USA", "NY", Instant.ofEpochMilli(1000)),
                new Order("Eric", "USA", "LA", Instant.ofEpochMilli(1000)),
                new Order("Rooney", "UK", "MAN", Instant.ofEpochMilli(1000)));

        tabEnv.createTemporaryView(
                "Orders",
                dataStream);
        Table orders = tabEnv.from("Orders"); // schema (a, b, c, rowtime)

        /**
         * 1) AddColumns
         * +----+----------+---------+------------+-------------------------------+-------------------+
         * | op |        a |       b |          c |                       rowtime |              desc |
         * +----+----------+---------+------------+-------------------------------+-------------------+
         * | +I |    Roger |   China |   Shanghai | 1970-01-01 08:00:01.000000000 |     Shanghaisunny |
         * | +I |     Andy |   China |   Shanghai | 1970-01-01 08:00:01.000000000 |     Shanghaisunny |
         * | +I |     John |     USA |         NY | 1970-01-01 08:00:01.000000000 |           NYsunny |
         * | +I |     Eric |     USA |         LA | 1970-01-01 08:00:01.000000000 |           LAsunny |
         * | +I |   Rooney |      UK |        MAN | 1970-01-01 08:00:01.000000000 |          MANsunny |
         * +----+----------+---------+------------+-------------------------------+-------------------+
         */
        Table result = orders.addColumns(concat($("c"), "sunny").as("desc"));
        result.execute().print();

        /**
         * 2) AddOrReplaceColumns
         */
        result = orders.addOrReplaceColumns(concat($("c"), "sunny").as("desc"));
        result.execute().print();

        /**
         * 3) DropColumns
         * +----+--------------------------------+-------------------------------+
         * | op |                              a |                       rowtime |
         * +----+--------------------------------+-------------------------------+
         * | +I |                          Roger | 1970-01-01 08:00:01.000000000 |
         * | +I |                           Andy | 1970-01-01 08:00:01.000000000 |
         * | +I |                           John | 1970-01-01 08:00:01.000000000 |
         * | +I |                           Eric | 1970-01-01 08:00:01.000000000 |
         * | +I |                         Rooney | 1970-01-01 08:00:01.000000000 |
         * +----+--------------------------------+-------------------------------+
         */
        result = orders.dropColumns($("b"), $("c"));
        result.execute().print();

        /**
         * 4) RenameColumns
         * +----+--------+-------+----------+-------------------------------+
         * | op |      a |    b2 |       c2 |                       rowtime |
         * +----+--------+-------+----------+-------------------------------+
         * | +I |  Roger | China | Shanghai | 1970-01-01 08:00:01.000000000 |
         * | +I |   Andy | China | Shanghai | 1970-01-01 08:00:01.000000000 |
         * | +I |   John |   USA |       NY | 1970-01-01 08:00:01.000000000 |
         * | +I |   Eric |   USA |       LA | 1970-01-01 08:00:01.000000000 |
         * | +I | Rooney |    UK |      MAN | 1970-01-01 08:00:01.000000000 |
         * +----+--------+-------+----------+-------------------------------+
         */
        result = orders.renameColumns($("b").as("b2"), $("c").as("c2"));
        result.execute().print();
    }

    /**
     * Aggregation 示例
     *
     * @param sEnv
     * @param tabEnv
     * @throws Exception
     */
    public static void aggregationSamples(StreamExecutionEnvironment sEnv, StreamTableEnvironment tabEnv) throws Exception {
        // DataStream
        DataStream<Order> dataStream = sEnv.fromElements(
                new Order("Roger", "China", "Shanghai", Instant.ofEpochMilli(1000)),
                new Order("Andy", "China", "Shanghai", Instant.ofEpochMilli(1000)),
                new Order("John", "USA", "NY", Instant.ofEpochMilli(1000)),
                new Order("Eric", "USA", "LA", Instant.ofEpochMilli(1000)),
                new Order("Rooney", "UK", "MAN", Instant.ofEpochMilli(1000)));

        tabEnv.createTemporaryView(
                "Orders",
                dataStream);
        Table orders = tabEnv.from("Orders"); // schema (a, b, c, rowtime)

        /**
         * 1) GroupBy
         * +----+--------------------------------+----------------------+
         * | op |                              a |                    d |
         * +----+--------------------------------+----------------------+
         * | +I |                           Eric |                    1 |
         * | +I |                          Roger |                    1 |
         * | +I |                         Rooney |                    1 |
         * | +I |                           Andy |                    1 |
         * | +I |                           John |                    1 |
         * +----+--------------------------------+----------------------+
         */
        Table result = orders.groupBy($("a")).select($("a"), $("b").count().as("d"));
        result.execute().print();
    }

    public static class Order {
        public String a;
        public String b;
        public String c;
        public Instant rowtime;

        public Order(String a, String b, String c, Instant rowtime) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.rowtime = rowtime;
        }

        public Order() {
        }
    }
}
