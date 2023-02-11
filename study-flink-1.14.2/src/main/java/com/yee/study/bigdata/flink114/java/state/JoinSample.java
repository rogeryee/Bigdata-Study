package com.yee.study.bigdata.flink114.java.state;

import lombok.AllArgsConstructor;
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
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 使用 State 实现数据join的示例
 *
 * @author Roger.Yi
 */
public class JoinSample {

    public static void main(String[] args) throws Exception {
        // ENV
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 模拟两个数据流，每个数据流，都是每隔 0.5s 发送一条数据
        // OrderPrice: String = 123,拖把,30.0
        DataStreamSource<String> orderPriceDS = environment.addSource(new FileSource("/Users/cntp/MyWork/yee/bigdata-study/study-flink-1.14.2/data/order_price.txt"));

        // OrderLocation: String = 123,2021-11-11 10:11:12,江苏
        DataStreamSource<String> orderLocationDS = environment.addSource(new FileSource("/Users/cntp/MyWork/yee/bigdata-study/study-flink-1.14.2/data/order_location.txt"));

        // orderPriceDS 和 orderLocationDS  分别按照 OrderId 分组
        KeyedStream<OrderPrice, Long> keyedOrderPriceDS = orderPriceDS.map(OrderPrice::deserialize)
                .keyBy(OrderPrice::getOrderId);
        KeyedStream<OrderLocation, Long> keyedOrderLocation2DS = orderLocationDS.map(OrderLocation::deserialize)
                .keyBy(OrderLocation::getOrderId);

        // 注意：这里是keyBy后的操作，所以每一个key都有一个ValueState
        // keyedOrderPriceDS 和 keyedOrderLocation2DS 做 connect 动作
        // 目的是： 通过 state 实现这两个 数据流的 join 效果
        // select a.* , b.* from a join b on a.orderid = b.orderid;
        keyedOrderPriceDS.connect(keyedOrderLocation2DS).flatMap(new OrderJoinFunction());//.print();

        // run
        environment.execute("FlinkState_OrderJoin");
    }

    /**
     * 数据流1 和 数据流2 都是按照 orderId 进行KeyBy分组
     * 假设数据流1 接收到了 orderid = 1 的数据记录， 但是 数据流2 的 orderid = 1 还没有收到
     * 先接收到的不能完成链接的，则作为 state 存储起来
     * 当接收到的数据，能完成链接的时候，先从 state 获取记录，然后完成链接
     */
    static class OrderJoinFunction extends RichCoFlatMapFunction<OrderPrice, OrderLocation, Tuple2<OrderPrice, OrderLocation>> {

        // 存储两个流中， 未进行 join 的数据
        private ValueState<OrderPrice> orderPriceState;
        private ValueState<OrderLocation> orderLocationState;

        @Override
        public void open(Configuration parameters) {
            this.orderPriceState = getRuntimeContext().getState(new ValueStateDescriptor<OrderPrice>("price", OrderPrice.class));
            this.orderLocationState = getRuntimeContext().getState(new ValueStateDescriptor<OrderLocation>("location", OrderLocation.class));
        }

        // 处理是左边流，左表的数据
        @Override
        public void flatMap1(OrderPrice orderPrice, Collector<Tuple2<OrderPrice, OrderLocation>> collector) throws Exception {
            OrderLocation orderLocation = this.orderLocationState.value();
            if (orderLocation != null) {
                this.orderLocationState.clear();
                collector.collect(Tuple2.of(orderPrice, orderLocation));
            } else {
                this.orderPriceState.update(orderPrice);
            }
        }

        // 处理的是右边流，右表的数据
        @Override
        public void flatMap2(OrderLocation orderLocation, Collector<Tuple2<OrderPrice, OrderLocation>> collector) throws Exception {
            OrderPrice orderPrice = this.orderPriceState.value();
            if (orderPrice != null) {
                this.orderPriceState.clear();
                collector.collect(Tuple2.of(orderPrice, orderLocation));
            } else {
                this.orderLocationState.update(orderLocation);
            }
        }
    }

    /**
     * 基于文件的数据源
     */
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

    /**
     * 订单价格类（包含 订单ID、商品名称、价格）
     */
    @AllArgsConstructor
    @Data
    static class OrderPrice {
        private Long orderId;
        private String productName;
        private Double price;

        // 将 csv 中的一行数据转化成 OrderPrice 对象
        public static OrderPrice deserialize(String line) {
            String[] fields = line.split(",");
            return new OrderPrice(Long.parseLong(fields[0]), fields[1], Double.parseDouble(fields[2]));
        }
    }

    /**
     * 订单地址类（包含 订单ID、下单时间、下单地址）
     */
    @AllArgsConstructor
    @Data
    static class OrderLocation {
        private Long orderId;
        private String orderDate;
        private String address;

        // 将 csv 中的一行数据转化成 OrderLocation 对象
        public static OrderLocation deserialize(String line) {
            String[] fields = line.split(",");
            return new OrderLocation(Long.parseLong(fields[0]), fields[1], fields[2]);
        }
    }
}