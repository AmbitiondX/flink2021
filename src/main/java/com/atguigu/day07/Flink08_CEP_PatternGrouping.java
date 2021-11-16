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

public class Flink08_CEP_PatternGrouping {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
                .readTextFile("input/sensor.txt")
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
                // todo 模式组
                // times(2)会对模式组里的所有模式产生效果，并且会把每个模式的结果聚合在一起
                // {start=[WaterSensor(id=sensor_1, ts=2, vc=20), WaterSensor(id=sensor_1, ts=4, vc=40)], next=[WaterSensor(id=sensor_2, ts=3, vc=30), WaterSensor(id=sensor_2, ts=5, vc=50)]}
                .begin(Pattern
                        .<WaterSensor>begin("start")
                        .where(new SimpleCondition<WaterSensor>() {
                            @Override
                            public boolean filter(WaterSensor value) throws Exception {
                                return "sensor_1".equals(value.getId());
                            }
                        })
                        .next("next")
                        .where(new SimpleCondition<WaterSensor>() {
                            @Override
                            public boolean filter(WaterSensor value) throws Exception {
                                return "sensor_2".equals(value.getId());
                            }
                        }))
                .times(2);

        //  在流上应用模式
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
