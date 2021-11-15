package com.atguigu.day06.keystate;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink07_OperatorState_BroadcastState {
    public static void main(String[] args) throws Exception {
        // 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        // 获取两个流
        DataStreamSource<String> lh7777 = env.socketTextStream("localhost", 7777);
        DataStreamSource<String> lh8888 = env.socketTextStream("localhost", 8888);

        // 定义广播状态
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("state", String.class, String.class);

        // 广播状态
        BroadcastStream<String> broadcast = lh7777.broadcast(mapStateDescriptor);

        // 连接两条流
        BroadcastConnectedStream<String, String> connect = lh8888.connect(broadcast);

        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 获取广播变量
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                // 提取状态中的数据
                String aSwitch = broadcastState.get("switch");
                if ("1".equals(aSwitch)) {
                    System.out.println(value + ":写入数据到kafka");
                } else if ("2".equals(aSwitch)) {
                    System.out.println(value + ":写入数据到redis");
                } else {
                    System.out.println(value + ":写入数据到hbase");
                }
            }

            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                // 提取状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                //向状态中存入数据
                broadcastState.put("switch",value);
            }
        }).print();

        env.execute();
    }
}
