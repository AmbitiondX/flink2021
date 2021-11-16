package com.atguigu.day07;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Flink06_CEP_Greed {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
                .readTextFile("input/sensor02.txt")
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>
                        () {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                }));

        // 定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("start")
                // todo 循环模式的贪婪性 greedy() 在组合模式情况下, 对次数的处理尽快能获取最多个的那个次数, 就是贪婪!当一个事件同时满足两个模式的时候起作用.
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "sensor_1".equals(waterSensor.getId());
                    }
                })
                .times(2,3)
                .greedy()
                .next("end")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value.getVc() == 30;
                    }
                });
        /*
            sensor_1,1,10
            sensor_1,2,20
            // sensor_1,3,30  在匹配的的时候, 既能匹配第一个模式也可以匹配的第二个模式,
            // 由于第一个模式使用量词则使用greedy的时候会优先匹配第一个模式, 因为要尽可能多的次数
            sensor_1,3,30
            sensor_2,4,30
            sensor_1,4,40
            sensor_2,5,50
            sensor_2,6,60
         */
        //注意:1.一般贪婪比非贪婪的结果要少!
        //    2.模式组不能设置为greedy


        //  在刘尚应用模式
        PatternStream<WaterSensor> waterSensorPatternStream = CEP.pattern(waterSensorStream, pattern);

        // 获取匹配到的结果
        waterSensorPatternStream.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                return map.toString();
            }
        }).print();

        env.execute();
    }
}
