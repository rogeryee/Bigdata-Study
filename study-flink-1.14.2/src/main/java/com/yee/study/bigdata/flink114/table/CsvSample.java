package com.yee.study.bigdata.flink114.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 基于 Flink Table，读取csv内容并输出到新的csv的示例
 *
 * @author Roger.Yi
 */
public class CsvSample {

    public static void main(String[] args) {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tabEnv = TableEnvironment.create(environmentSettings);

        // Source Csv Schema
        final Schema csvSchema = Schema.newBuilder()
                                       .column("id", DataTypes.BIGINT())
                                       .column("name", DataTypes.STRING())
                                       .column("age", DataTypes.INT())
                                       .build();

        // Create table
        tabEnv.createTemporaryTable("csv_sample", TableDescriptor.forConnector("filesystem")
                                                                 .schema(csvSchema)
                                                                 .option("path", "file:///Users/cntp/MyWork/yee/bigdata-study/study-flink-1.14.2/data/table_csv_sample.csv")
                                                                 .format(FormatDescriptor.forFormat("csv")
                                                                                         .option("field-delimiter", ",")
                                                                                         .build())
                                                                 .build());

//        Table csv_sample = tabEnv.from("csv_sample").filter("age > 25").select("id,name,age");
        Table csv_sample = tabEnv.sqlQuery("select id, name, age from csv_sample where age > 25");
        csv_sample.printSchema();

        // Sink to csv
        tabEnv.createTemporaryTable("csv_sample_sink_csv", TableDescriptor.forConnector("filesystem")
                                                                      .schema(csvSchema)
                                                                      .option("path", "file:///Users/cntp/MyWork/yee/bigdata-study/study-flink-1.14.2/output/table_csv_sample_sink.csv")
                                                                      .format(FormatDescriptor.forFormat("csv")
                                                                                              .option("field-delimiter", "\t")
                                                                                              .build())
                                                                      .build());
        csv_sample.executeInsert("csv_sample_sink_csv");

        // Sink to Print
        tabEnv.createTemporaryTable("csv_sample_sink_print", TableDescriptor.forConnector("print")
                                                                      .schema(csvSchema)
                                                                      .build());
        csv_sample.executeInsert("csv_sample_sink_print");
    }
}
