package com.yee.study.bigdata.flink114.java.table;

import java.time.Instant;

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
public class User {
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
