package com.atguigu.day07.project;

import com.atguigu.day04.project.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Flink02_CEP_Project_OrderWatch {
    // todo 统计创建订单到下单中间超过15分钟的超时数据以及正常的数据。
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        KeyedStream<OrderEvent, Long> orderEventLongKeyedStream = env
                .readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] split = line.split(",");
                    return new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000;
                    }
                }))
                .keyBy(k -> k.getOrderId());

        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .next("pay")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        // 在流上应用模式
        PatternStream<OrderEvent> pattern1 = CEP.pattern(orderEventLongKeyedStream, pattern);

        SingleOutputStreamOperator<String> select = pattern1
                .select(
                        new OutputTag<String>("output") {
                        },
                        new PatternTimeoutFunction<OrderEvent, String>() {
                            @Override
                            public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                                return pattern.toString();
                            }
                        },
                        new PatternSelectFunction<OrderEvent, String>() {
                            @Override
                            public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                                return pattern.toString();
                            }
                        });

        select.print("主流");
        select.getSideOutput(new OutputTag<String>("output"){}).print("侧输出");

        env.execute();
    }
}
