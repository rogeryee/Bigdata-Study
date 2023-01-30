package com.yee.study.bigdata.flink114.window.support;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * MyEvent 定义Bean
 *
 * @author Roger.Yi
 */
@Data
@AllArgsConstructor
public class MyEvent {
    private String name;
    private Long eventTime;
    private String type;

    @Override
    public String toString() {
        return "MyEvent{" +
                "name='" + name + '\'' +
                ", eventTime=" + eventTime +
                ", type='" + type + '\'' +
                '}';
    }
}
