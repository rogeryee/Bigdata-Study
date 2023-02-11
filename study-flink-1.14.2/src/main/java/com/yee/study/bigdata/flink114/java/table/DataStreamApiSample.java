package com.yee.study.bigdata.flink114.java.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.time.Instant;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

/**
 * DataStream 和 Table 配合使用的示例2
 *
 * @author Roger.Yi
 */
public class DataStreamApiSample {

    public static void main(String[] args) throws Exception {
        // Stream env
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // Table env
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(sEnv);

        // fromDataStream 示例
//        fromDataStreamSamples(sEnv, tabEnv);

        // createTemporaryVie 示例
//        createTemporaryViewSamples(sEnv, tabEnv);

        // toDataStream 示例
//        toDataStreamSamples(sEnv, tabEnv);

        // fromChangelogStream 示例
//        fromChangelogStreamSamples(sEnv, tabEnv);

        // fromChangelogStream 示例
//        toChangelogStreamSamples(sEnv, tabEnv);

        // StreamStatementSet 示例
        streamStatementSetSamples(sEnv, tabEnv);
    }

    /**
     * fromDataStream 示例
     *
     * @param sEnv
     * @param tabEnv
     */
    public static void fromDataStreamSamples(StreamExecutionEnvironment sEnv, StreamTableEnvironment tabEnv) {
        // DataStream
        DataStream<User> ds = sEnv.fromElements(new User("Roger", 10, Instant.ofEpochMilli(1000)),
                                                new User("Andy", 3, Instant.ofEpochMilli(1001)),
                                                new User("John", 6, Instant.ofEpochMilli(1002)));
        /**
         * 1) fromDataStream(DataStream)
         * 根据自定义类型获取字段
         * (
         *   `name` STRING,
         *   `score` INT NOT NULL,
         *   `event_time` TIMESTAMP_LTZ(9)
         * )
         */
        Table table1 = tabEnv.fromDataStream(ds);
        table1.printSchema();

        /**
         * 2) fromDataStream(DataStream, Schema)
         * 自动获取字段类型、并增加一个计算字段（基于proctime）
         * (
         *   `name` STRING,
         *   `score` INT NOT NULL,
         *   `event_time` TIMESTAMP_LTZ(9),
         *   `proc_time` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
         * )
         */
        Table table2 = tabEnv.fromDataStream(
                ds,
                Schema.newBuilder()
                      .columnByExpression("proc_time", "PROCTIME()")
                      .build());
        table2.printSchema();

        /**
         * 3) fromDataStream(DataStream, Schema)
         * 自动获取字段类型、并增加一个计算字段（基于proctime）和一个自定义的watermark策略
         * (
         *   `name` STRING,
         *   `score` INT NOT NULL,
         *   `event_time` TIMESTAMP_LTZ(9),
         *   `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS CAST(event_time AS TIMESTAMP_LTZ(3)),
         *   WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS rowtime - INTERVAL '10' SECOND
         * )
         */
        Table table3 = tabEnv.fromDataStream(
                ds,
                Schema.newBuilder()
                      .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
                      .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
                      .build());
        table3.printSchema();

        /**
         * 4) fromDataStream(DataStream, Schema)
         * 手动定义字段类型
         * (
         *   `event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
         *   `name` STRING,
         *   `score` INT
         * )
         */
        Table table4 = tabEnv.fromDataStream(
                ds,
                Schema.newBuilder()
                      .column("event_time", "TIMESTAMP_LTZ(3)")
                      .column("name", "STRING")
                      .column("score", "INT")
                      .watermark("event_time", "SOURCE_WATERMARK()")
                      .build());
        table4.printSchema();

        // DataStream with GenericType
        DataStream<UserGenericType> ds2 = sEnv.fromElements(new UserGenericType("Roger", 10),
                                                            new UserGenericType("Andy", 3),
                                                            new UserGenericType("John", 6));

        /**
         * 5) fromDataStream(DataStream)
         * RAW type，默认字段名为 f0
         * (
         *   `f0` RAW('com.yee.study.bigdata.flink114.table.DataStreamSample2$UserGenericType', '...')
         * )
         */
        Table table5 = tabEnv.fromDataStream(ds2);
        table5.printSchema();

        /**
         * 6) fromDataStream(DataStream, Schema)
         * 使用 as 方法自定义类型名
         * (
         *   `user` *com.yee.study.bigdata.flink114.table.DataStreamSample2$UserGenericType<`name` STRING, `score` INT NOT NULL>*
         * )
         */
        Table table6 = tabEnv.fromDataStream(
                ds2,
                Schema.newBuilder()
                      .column("f0", DataTypes.of(UserGenericType.class))
                      .build())
                             .as("user");
        table6.printSchema();

        /**
         * 7) fromDataStream(DataStream, Schema)
         * 自定义的指定类型
         * (
         *   `user` *com.yee.study.bigdata.flink114.table.DataStreamSample2$UserGenericType<`name` STRING, `score` INT>*
         * )
         */
        Table table7 = tabEnv.fromDataStream(
                ds2,
                Schema.newBuilder()
                      .column(
                              "f0",
                              DataTypes.STRUCTURED(
                                      UserGenericType.class,
                                      DataTypes.FIELD("name", DataTypes.STRING()),
                                      DataTypes.FIELD("score", DataTypes.INT())))
                      .build())
                             .as("user");
        table7.printSchema();
    }

    /**
     * createTemporaryView 示例
     *
     * @param sEnv
     * @param tabEnv
     */
    public static void createTemporaryViewSamples(StreamExecutionEnvironment sEnv, StreamTableEnvironment tabEnv) {
        // DataStream
        DataStream<Tuple2<Long, String>> dataStream = sEnv.fromElements(
                Tuple2.of(12L, "Alice"),
                Tuple2.of(0L, "Bob"));

        /**
         * 1) 自动获取字段
         * (
         *   `f0` BIGINT NOT NULL,
         *   `f1` STRING
         * )
         */
        tabEnv.createTemporaryView("MyView", dataStream);
        tabEnv.from("MyView").printSchema();

        /**
         * 2) 自定义字段
         * (
         *   `f0` BIGINT,
         *   `f1` STRING
         * )
         */
        tabEnv.createTemporaryView(
                "MyView2",
                dataStream,
                Schema.newBuilder()
                      .column("f0", "BIGINT")
                      .column("f1", "STRING")
                      .build());
        tabEnv.from("MyView2").printSchema();

        /**
         * 3) 基于Table创建
         * (
         *   `id` BIGINT NOT NULL,
         *   `name` STRING
         * )
         */
        tabEnv.createTemporaryView(
                "MyView3",
                tabEnv.fromDataStream(dataStream).as("id", "name"));
        tabEnv.from("MyView3").printSchema();
    }

    /**
     * toDataStream 示例
     *
     * @param sEnv
     * @param tabEnv
     */
    public static void toDataStreamSamples(StreamExecutionEnvironment sEnv, StreamTableEnvironment tabEnv) throws Exception {
        // Table
        tabEnv.executeSql(
                "CREATE TABLE GeneratedTable "
                        + " ( "
                        + " name STRING,"
                        + " score INT,"
                        + " event_time TIMESTAMP_LTZ(3),"
                        + "WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
                        + " ) "
                        + " WITH ('connector'='datagen')");
        Table table = tabEnv.from("GeneratedTable");

        /**
         * ROW 类型
         * 1> +I[cfa5dfab5116817ff7bacb07315558d12b85917a2f59a662f4c83e7c5615d02f1f75dc9d9510cd0e55a91a227dae4aa485ec, 1014879561, 2023-02-10T05:16:54.130Z]
         * 2> +I[cde737f4cea803c822c69b6942eceec430802852fc0766b4a0c2d91c4c02538b68c6691cc10662e08b49eb6ca59513849cf9, 569986090, 2023-02-10T05:16:54.130Z]
         */
        DataStream<Row> dataStream = tabEnv.toDataStream(table);
        dataStream.print();

        /**
         * User 类型
         * 10> com.yee.study.bigdata.flink114.table.DataStreamApiSample$User@59097a33
         * 7> com.yee.study.bigdata.flink114.table.DataStreamApiSample$User@2a730f7c
         */
        DataStream<User> dataStream2 = tabEnv.toDataStream(table, User.class);
        dataStream2.print();

        sEnv.execute("toDataStreamSamples");
    }

    /**
     * fromChangelogStream 示例
     *
     * @param sEnv
     * @param tabEnv
     */
    public static void fromChangelogStreamSamples(StreamExecutionEnvironment sEnv, StreamTableEnvironment tabEnv) {
        /**
         * +----+--------------------------------+-------------+
         * | op |                           name |       score |
         * +----+--------------------------------+-------------+
         * | +I |                            Bob |           5 |
         * | +I |                          Alice |          12 |
         * | -D |                          Alice |          12 |
         * | +I |                          Alice |         100 |
         * +----+--------------------------------+-------------+
         */
        DataStream<Row> dataStream1 = sEnv.fromElements(
                Row.ofKind(RowKind.INSERT, "Alice", 12),
                Row.ofKind(RowKind.INSERT, "Bob", 5),
                Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));
        Table table1 = tabEnv.fromChangelogStream(dataStream1);
        tabEnv.createTemporaryView("InputTable1", table1);
        tabEnv.executeSql("select f0 as name, sum(f1) as score from InputTable1 group by f0").print();

        /**
         * +----+--------------------------------+-------------+
         * | op |                           name |      EXPR$1 |
         * +----+--------------------------------+-------------+
         * | +I |                            Bob |           5 |
         * | +I |                          Alice |          12 |
         * | -U |                          Alice |          12 |
         * | +U |                          Alice |         100 |
         * +----+--------------------------------+-------------+
         */
        DataStream<Row> dataStream2 = sEnv.fromElements(
                Row.ofKind(RowKind.INSERT, "Alice", 12),
                Row.ofKind(RowKind.INSERT, "Bob", 5),
                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));
        Table table2 = tabEnv.fromChangelogStream(
                dataStream2,
                Schema.newBuilder().primaryKey("f0").build(), ChangelogMode.upsert());
        tabEnv.createTemporaryView("InputTable2", table2);
        tabEnv.executeSql("select f0 as name, sum(f1) from InputTable2 group by f0").print();
    }

    /**
     * fromChangelogStream 示例
     *
     * @param sEnv
     * @param tabEnv
     */
    public static void toChangelogStreamSamples(StreamExecutionEnvironment sEnv, StreamTableEnvironment tabEnv) throws Exception {
        tabEnv.executeSql(
                "CREATE TABLE GeneratedTable "
                        + " ( "
                        + " name STRING,"
                        + " score INT,"
                        + " event_time TIMESTAMP_LTZ(3),"
                        + "WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
                        + " ) "
                        + " WITH ('connector'='datagen')");
        Table table = tabEnv.from("GeneratedTable");

        /**
         * +I[Bob, 12]
         * +I[Alice, 12]
         * -U[Alice, 12]
         * +U[Alice, 14]
         */
        Table simpleTable = tabEnv
                .fromValues(row("Alice", 12), row("Alice", 2), row("Bob", 12))
                .as("name", "score")
                .groupBy($("name"))
                .select($("name"), $("score").sum());
        tabEnv.toChangelogStream(simpleTable)
              .executeAndCollect()
              .forEachRemaining(System.out::println);

        /**
         * [name, score, event_time]
         * [name, score, event_time]
         */
        DataStream<Row> dataStream = tabEnv.toChangelogStream(table);
        dataStream.process(new ProcessFunction<Row, Void>() {
            @Override
            public void processElement(Row value, Context ctx, Collector<Void> out) throws Exception {
                System.out.println(value.getFieldNames(true));
                assert ctx.timestamp() == value.<Instant>getFieldAs("event_time").toEpochMilli();
            }
        });

        sEnv.execute("toChangelogStream");
    }

    /**
     * StreamStatementSet 示例
     *
     * +I[201449218, false]
     * +I[755060145, true]
     * +I[-1345892487, false]
     * +I[1]
     * +I[2]
     * +I[3]
     *
     * @param sEnv
     * @param tabEnv
     * @throws Exception
     */
    public static void streamStatementSetSamples(StreamExecutionEnvironment sEnv, StreamTableEnvironment tabEnv) throws Exception {
        TableDescriptor sourceDescriptor = TableDescriptor.forConnector("datagen")
                                                          .option("number-of-rows", "3")
                                                          .schema(Schema.newBuilder()
                                                                        .column("myCol", DataTypes.INT())
                                                                        .column("myOtherCol", DataTypes.BOOLEAN())
                                                                        .build())
                                                          .build();
        TableDescriptor sinkDescriptor = TableDescriptor.forConnector("print").build();

        // Add a table pipeline
        StreamStatementSet statementSet = tabEnv.createStatementSet();
        Table tableFromSource = tabEnv.from(sourceDescriptor);
        statementSet.addInsert(sinkDescriptor, tableFromSource);

        // 1, 2, 3
        DataStream<Integer> dataStream = sEnv.fromElements(1, 2, 3);
        Table tableFromStream = tabEnv.fromDataStream(dataStream);
        statementSet.addInsert(sinkDescriptor, tableFromStream);

        statementSet.attachAsDataStream();
        sEnv.execute();
    }

    /**
     * 用户自定义类型 User-Defined Data Types
     * <p>
     * 必须符合以下条件：
     * 1）class 必须是public 修饰的，或者是static修饰的，且不能是抽象类
     * 2）class 必须提供一个无参的构造方法，或者一个赋值所有字段的完整构造函数
     * 3）所有的字段要么用public 修饰， 要么提供public 的get/set 方法，二者必须满足其一
     * 4）所有字段必须通过反射提取隐式映射到数据类型，或者使用@DataTypeHint注释显式映射到数据类型。
     * 5）声明为静态或暂态的字段将被忽略
     */
    public static class User {
        public String name;
        public int score;
        public Instant event_time;

        public User() {
        }

        public User(String name, int score, Instant event_time) {
            this.name = name;
            this.score = score;
            this.event_time = event_time;
        }
    }

    /**
     * 与 User 类差异在于，UserGenericType 缺少了默认构造器，导致 fromDataStream 自动解析类型时认为 UserGenericType是一个 generic type。
     * 所以最后会解析成一个 RAW 类型
     */
    public static class UserGenericType {
        public String name;
        public int score;

        public UserGenericType(String name, int score) {
            this.name = name;
            this.score = score;
        }
    }
}
