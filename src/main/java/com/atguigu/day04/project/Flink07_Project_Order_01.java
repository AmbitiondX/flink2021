package com.atguigu.day04.project;

import com.atguigu.day04.project.bean.OrderEvent;
import com.atguigu.day04.project.bean.TxEvent;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class Flink07_Project_Order_01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 1. 读取Order流
        DataStreamSource<String> source1 = env.readTextFile("input/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderSource = source1.map(line -> {
            String[] split = line.split(",");
            return new OrderEvent(
                    Long.parseLong(split[0]),
                    split[1],
                    split[2],
                    Long.parseLong(split[3])
            );
        });

        DataStreamSource<String> source2 = env.readTextFile("input/ReceiptLog.csv");
        SingleOutputStreamOperator<TxEvent> txEvent = source2.map(line -> {
            String[] split = line.split(",");
            return new TxEvent(
                    split[0],
                    split[1],
                    Long.parseLong(split[2])
            );
        });
        ConnectedStreams<OrderEvent, TxEvent> connect = orderSource.connect(txEvent);

        connect.keyBy("txId","txId")
                .process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
                    HashMap<String, OrderEvent> orderMap = new HashMap<>();
                    HashMap<String, TxEvent> txMap = new HashMap<>();
                    @Override
                    public void processElement1(OrderEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) throws Exception {
                        if (txMap.containsKey(value.getTxId())) {
                            System.out.println("订单：" + value + "支付成功");
                            txMap.remove(value.getTxId());
                        } else {
                            orderMap.put(value.getTxId(),value);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) throws Exception {
                        if (orderMap.containsKey(value.getTxId())) {
                            System.out.println("订单：" + orderMap.get(value.getTxId()) + "支付成功");
                            txMap.remove(value.getTxId());
                        } else {
                            txMap.put(value.getTxId(),value);
                        }
                    }
                })
                .print();


        env.execute();
    }
}
