package com.yee.study.bigdata.flink114.window.support;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.TimestampAssigner;

/**
 * MyEvent 对象 EventTime 解析器
 *
 * @author Roger.Yi
 */
@Slf4j
public class MyEventTimestampExtractor implements TimestampAssigner<MyEvent> {
    @Override
    public long extractTimestamp(MyEvent event, long recordTimestamp) {
        log.info("TimestampExtractor: " + event);
        return event.getEventTime();
    }
}
