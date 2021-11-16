package com.atguigu.day07.project;

import com.atguigu.day07.project.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;


public class Flink01_CEP_Project_Login_withDStreamApi {
    public static void main(String[] args) throws Exception {
        // todo 用户2秒内连续两次及以上登录失败则判定为恶意登录。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        KeyedStream<LoginEvent, Long> loginEventLongKeyedStream = env
                .readTextFile("input/LoginLog.csv")
                .map(line -> {
                    String[] split = line.split(",");
                    return new LoginEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime();
                    }
                }))
                .keyBy(k -> k.getUserId());

        loginEventLongKeyedStream.process(new KeyedProcessFunction<Long, LoginEvent, String>() {
            private ListState<LoginEvent> listState;
            private ValueState<Long> timer;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("listState", LoginEvent.class));
                timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class,Long.MIN_VALUE));
            }

            @Override
            public void processElement(LoginEvent value, KeyedProcessFunction<Long, LoginEvent, String>.Context ctx, Collector<String> out) throws Exception {
                if ("fail".equals(value.getEventType())) {

                    if (timer.value() == Long.MIN_VALUE) {
                        listState.add(value);
                        timer.update(value.getEventTime());
                    } else {
                        if (value.getEventTime() - timer.value() <= 2){
                            listState.add(value);
                            timer.update(value.getEventTime());
                            out.collect(listState.get().toString());
                        } else {
                            timer.update(value.getEventTime());
                        }
                    }
                } else {
                    timer.clear();
                }
            }
        }).print();



        env.execute();

    }
}
