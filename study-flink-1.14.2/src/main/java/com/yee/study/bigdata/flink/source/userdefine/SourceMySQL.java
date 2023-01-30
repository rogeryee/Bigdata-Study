package com.yee.study.bigdata.flink.source.userdefine;

import lombok.Data;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 自定义数据源，从 mysql 中读取数据
 * <p>
 * 自定义一个数据源，有两种方式：
 * 1、implements SourceFunction
 * 2、extends RichSourceFunction  更富有： 功能跟强大， 提供一些生命周期方法
 *
 * @author Roger.Yi
 */
public class SourceMySQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Student> studentDS = executionEnvironment.addSource(new MySQLSource()).setParallelism(1);

        // 计算城市人数
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = studentDS.map(new MapFunction<Student, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Student student) throws Exception {
                return Tuple2.of(student.getCity(), 1);
            }
        }).keyBy(0).sum(1).setParallelism(1);

        // 输出
        resultDS.print().setParallelism(1);

        executionEnvironment.execute("SourceMySQL running.");
    }
}

/**
 * 自定义的数据源，实现的逻辑效果： 每隔 1s 读取到所有数据输出到 Flink 程序中执行计算
 */
class MySQLSource extends RichSourceFunction<Student> {

    private boolean running = false;

    private Connection connection;

    private PreparedStatement preparedStatement;

    /**
     * 当初始化 MySQLUDSource 这个实例之后，会立即执行 open 一次
     * 做初始化：链接，状态恢复，重量级大对象等等
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String driver = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/insight_task?useSSL=false";
        String username = "root";
        String password = "12345678";
        String sql = "select id, name, city from student";

        // 初始化链接
        Class.forName(driver);
        connection = DriverManager.getConnection(url, username, password);
        preparedStatement = connection.prepareStatement(sql);

        running = true;
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        while (running) {
            ResultSet resultSet = preparedStatement.executeQuery();
            Student student;
            while (resultSet.next()) {
                student = new Student();
                student.setId(resultSet.getInt("id"));
                student.setName(resultSet.getString("name"));
                student.setCity(resultSet.getString("city"));
                ctx.collect(student);
            }
            Thread.sleep(3000);
        }
    }

    @Override
    public void cancel() {
        running = false;
        System.out.println("MySQL source closed.");
    }
}

@Data
class Student {

    private int id;

    private String name;

    private String city;

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}
