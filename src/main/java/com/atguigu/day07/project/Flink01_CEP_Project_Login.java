package com.atguigu.day07.project;

import com.atguigu.day07.project.bean.LoginEvent;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;


public class Flink01_CEP_Project_Login {
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
                        return element.getEventTime() * 1000;
                    }
                }))
                .keyBy(k -> k.getUserId());

        Pattern<LoginEvent, LoginEvent> fail = Pattern
                .<LoginEvent>begin("fail")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .timesOrMore(2)
                .consecutive()
                .within(Time.seconds(2));

        PatternStream<LoginEvent> pattern = CEP.pattern(loginEventLongKeyedStream, fail);

        pattern.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                return pattern.toString();
            }
        }).print();

        env.execute();

    }
}
