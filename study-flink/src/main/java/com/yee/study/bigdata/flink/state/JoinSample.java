package com.yee.study.bigdata.flink.state;

import lombok.Data;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 使用 State 实现数据join的示例
 *
 * @author Roger.Yi
 */
public class JoinSample {

    public static void main(String[] args) throws Exception {
        // TODO_MA 马中华 注释：
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(3);

        // TODO_MA 马中华 注释： 模拟两个数据流，每个数据流，都是每隔 0.5s 发送一条数据
        // TODO_MA 马中华 注释： String = 123,拖把,30.0
        DataStreamSource<String> order1DS = environment.addSource(new FileSource("/Users/cntp/MyWork/yee/bigdata-study/study-flink/data/order_price.txt"));
        // TODO_MA 马中华 注释： String = 123,2021-11-11 10:11:12,江苏
        DataStreamSource<String> order2DS = environment.addSource(new FileSource("/Users/cntp/MyWork/yee/bigdata-study/study-flink/data/order_location.txt"));

        // TODO_MA 马中华 注释： order1DS 按照 OrderId 分组
        // TODO_MA 马中华 注释： order2DS 按照 OrderId 分组
        KeyedStream<OrderPrice, Long> keyedOrderInfo1DS = order1DS.map(line -> OrderPrice.deserialize(line))
                .keyBy(orderInfo1 -> orderInfo1.getOrderId());
        KeyedStream<OrderLocation, Long> keyedOrderInfo2DS = order2DS.map(line -> OrderLocation.deserialize(line))
                .keyBy(orderInfo2 -> orderInfo2.getOrderId());

        // TODO_MA 马中华 注释： keyedOrderInfo1DS 和 keyedOrderInfo2DS 做 connect 动作
        // TODO_MA 马中华 注释： 目的是： 通过 state 实现这两个 数据流的 join 效果
        // TODO_MA 马中华 注释： select a.* , b.* from a join b on a.orderid = b.orderid;
        keyedOrderInfo1DS.connect(keyedOrderInfo2DS).flatMap(new OrderJoinFunction()).print();
        // TODO_MA 马中华 注释： 为什么需要用到状态呢？
        // TODO_MA 马中华 注释： 假设数据流1 接收到了 orderid = 1 的数据记录， 但是 数据流2 的 orderid = 1 还没有收到
        // TODO_MA 马中华 注释： 先接收到的不能完成链接的，则作为 state 存储起来
        // TODO_MA 马中华 注释： 当接收到的数据，能完成链接的时候，先从 state 获取记录，然后完成链接
        // TODO_MA 马中华 注释： DS1[Info1]   DS2[Info2]

        // TODO_MA 马中华 注释：
        environment.execute("FlinkState_OrderJoin");
    }

    static class OrderJoinFunction extends RichCoFlatMapFunction<OrderPrice, OrderLocation, Tuple2<OrderPrice, OrderLocation>> {

        // TODO_MA 马中华 注释： 存储两个流中， 未进行 join 的数据
        private ValueState<OrderPrice> orderInfo1State;
        private ValueState<OrderLocation> orderInfo2State;

        @Override
        public void open(Configuration parameters) {
            orderInfo1State = getRuntimeContext().getState(new ValueStateDescriptor<OrderPrice>("info1", OrderPrice.class));
            orderInfo2State = getRuntimeContext().getState(new ValueStateDescriptor<OrderLocation>("info2", OrderLocation.class));
        }

        // TODO_MA 马中华 注释： 处理是左边流，左表的数据
        @Override
        public void flatMap1(OrderPrice orderInfo1, Collector<Tuple2<OrderPrice, OrderLocation>> collector) throws Exception {
            OrderLocation value2 = orderInfo2State.value();
            if (value2 != null) {
                orderInfo2State.clear();
                collector.collect(Tuple2.of(orderInfo1, value2));
            } else {
                orderInfo1State.update(orderInfo1);
            }
        }

        // TODO_MA 马中华 注释： 处理的是右边流，右表的数据
        @Override
        public void flatMap2(OrderLocation orderInfo2, Collector<Tuple2<OrderPrice, OrderLocation>> collector) throws Exception {
            OrderPrice value1 = orderInfo1State.value();
            if (value1 != null) {
                orderInfo1State.clear();
                collector.collect(Tuple2.of(value1, orderInfo2));
            } else {
                orderInfo2State.update(orderInfo2);
            }
        }
    }

    static class FileSource implements SourceFunction<String> {

        private String filePath;
        private InputStream inputStream;
        private BufferedReader bufferedReader;
        private Random random = new Random();

        FileSource(String filePath) {
            this.filePath = filePath;
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            bufferedReader = new BufferedReader(new FileReader(new File(filePath)));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                // 模拟发送数据
                TimeUnit.MILLISECONDS.sleep(random.nextInt(500));
                // 发送数据
                sourceContext.collect(line);
            }
            if (bufferedReader != null) {
                bufferedReader.close();
            }
            if (inputStream != null) {
                inputStream.close();
            }
        }

        @Override
        public void cancel() {
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Data
    static class OrderPrice {
        //订单ID
        private Long orderId;
        //商品名称
        private String productName;
        //价格
        private Double price;

        public OrderPrice() {
        }

        public OrderPrice(Long orderId, String productName, Double price) {
            this.orderId = orderId;
            this.productName = productName;
            this.price = price;
        }

        @Override
        public String toString() {
            return "OrderInfo1{" + "orderId=" + orderId + ", productName='" + productName + '\'' + ", price=" + price + '}';
        }

        // 将从文件中，读取到的一行数据转化成 OrderPrice
        public static OrderPrice deserialize(String line) {
            OrderPrice orderInfo1 = new OrderPrice();
            if (line != null && line.length() > 0) {
                String[] fields = line.split(",");
                orderInfo1.setOrderId(Long.parseLong(fields[0]));
                orderInfo1.setProductName(fields[1]);
                orderInfo1.setPrice(Double.parseDouble(fields[2]));
            }
            return orderInfo1;
        }
    }

    @Data
    static class OrderLocation {
        //订单ID
        private Long orderId;
        //下单时间
        private String orderDate;
        //下单地址
        private String address;

        public OrderLocation() {
        }

        public OrderLocation(Long orderId, String orderDate, String address) {
            this.orderId = orderId;
            this.orderDate = orderDate;
            this.address = address;
        }

        @Override
        public String toString() {
            return "OrderInfo2{" + "orderId=" + orderId + ", orderDate='" + orderDate + '\'' + ", address='" + address + '\'' + '}';
        }

        // TODO_MA 马中华 注释： 工具方法，将文件中读取到的一行数据转化成 OrderInfo2
        public static OrderLocation deserialize(String line) {
            OrderLocation orderInfo2 = new OrderLocation();
            if (line != null && line.length() > 0) {
                String[] fields = line.split(",");
                orderInfo2.setOrderId(Long.parseLong(fields[0]));
                orderInfo2.setOrderDate(fields[1]);
                orderInfo2.setAddress(fields[2]);
            }
            return orderInfo2;
        }
    }
}