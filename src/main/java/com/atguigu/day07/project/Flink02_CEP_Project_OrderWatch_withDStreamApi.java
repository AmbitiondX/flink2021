package com.atguigu.day07.project;

import com.atguigu.day04.project.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Flink02_CEP_Project_OrderWatch_withDStreamApi {
    // todo 统计创建订单到下单中间超过15分钟的超时数据以及正常的数据。
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        KeyedStream<OrderEvent, Long> orderEventLongKeyedStream = env
                .readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] split = line.split(",");
                    return new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]) * 1000);
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20)).withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getEventTime();
                    }
                }))
                .keyBy(k -> k.getOrderId());

        SingleOutputStreamOperator<String> process = orderEventLongKeyedStream.process(new KeyedProcessFunction<Long, OrderEvent, String>() {
            private ValueState<OrderEvent> createOrder;
            private ValueState<OrderEvent> payOrder;
            private ValueState<Long> timer;

            @Override
            public void open(Configuration parameters) throws Exception {
                createOrder = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createOrder", OrderEvent.class));
                payOrder = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("payOrder", OrderEvent.class));
                timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Long, OrderEvent, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                if (createOrder.value() == null) {
                    // 此时 没有createOrder信息
                    ctx.output(new OutputTag<String>("output" ){}, ctx.getCurrentKey() + "支付成功，但是没有订单信息，请检查系统");
                } else {
                    // 此时 没有payOrder信息
                    ctx.output(new OutputTag<String>("output" ){}, ctx.getCurrentKey()  + "下单成功，但是并未支付");
                }
//                createOrder.clear();
//                payOrder.clear();
//                timer.clear();
            }

            @Override
            public void processElement(OrderEvent value, KeyedProcessFunction<Long, OrderEvent, String>.Context ctx, Collector<String> out) throws Exception {
                if ("create".equals(value.getEventType())){
                    // value为创建订单
                    if (payOrder.value() == null) {
                        // payOrder为空，则先缓存自己
                        createOrder.update(value);
                    } else {
                        // payOrder不为空，判断是否超时
                        if (payOrder.value().getEventTime() - value.getEventTime() <= 15 * 60 * 1000){
                            out.collect(value.getOrderId() + ":支付成功");
                        }else {
                            out.collect("订单: " + value.getOrderId() + " 在超时时间完成的支付, 系统可能存在漏洞");
                        }
                    }
                }else {
                    // value为支付订单
                    if (createOrder.value() == null) {
                        // createOrder为空，则先缓存自己
                        payOrder.update(value);
                    } else {
                        // createOrder不为空，判断是否超时
                        if (value.getEventTime() - createOrder.value().getEventTime() <= 15 * 60 * 1000){
                            out.collect(value.getOrderId()  + ":支付成功");
                        }else {
                            out.collect("订单: " + value.getOrderId() + " 在超时时间完成的支付, 系统可能存在漏洞");
                        }
                    }
                }

                // 当第一个值进来之后，设置定时器
                if (timer.value() == null) {
                    timer.update(value.getEventTime() + 15 * 60 * 1000);
                    ctx.timerService().registerEventTimeTimer(timer.value());
                } else {
                    // 此时第二个值进来，则取消定时器
                    ctx.timerService().deleteEventTimeTimer(timer.value());
                    timer.clear();
                }
            }
        });

        process.print("主流");
        process.getSideOutput(new OutputTag<String>("output"){}).print("侧输出");

        env.execute();
    }
}
