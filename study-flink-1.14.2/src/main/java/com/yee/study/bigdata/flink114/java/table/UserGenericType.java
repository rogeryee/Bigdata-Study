package com.yee.study.bigdata.flink114.java.table;

/**
 * 与 User 类差异在于，UserGenericType 缺少了默认构造器，导致 fromDataStream 自动解析类型时认为 UserGenericType是一个 generic type。
 * 所以最后会解析成一个 RAW 类型
 */
public class UserGenericType {
    public String name;
    public int score;

    public UserGenericType(String name, int score) {
        this.name = name;
        this.score = score;
    }
}
