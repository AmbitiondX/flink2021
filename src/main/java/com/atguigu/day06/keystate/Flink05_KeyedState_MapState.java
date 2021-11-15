package com.atguigu.day06.keystate;

import com.atguigu.day02.source.WaterSensor;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;


public class Flink05_KeyedState_MapState {
    public static void main(String[] args) throws Exception {
        // todo 去重: 去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env
                .socketTextStream("localhost", 7777)
                .map(line -> {
                    String[] split = line.split(" ");
                    return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
                })
                .keyBy(k -> k.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

                    private MapState<HashMap,WaterSensor> mapState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<HashMap, WaterSensor>("mapState", HashMap.class,WaterSensor.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        // 创建一个hashMap存储value的时间和水位
                        HashMap<Long, Integer> hashMap = new HashMap<>();
                        hashMap.put(value.getTs(), value.getVc());
                        // 如果mapState中key包含hashMap，则过去重滤掉，否则写出
                        if (!mapState.contains(hashMap)){
                            mapState.put(hashMap,value);
                            out.collect(value);
                        } else {
                            System.out.println(value + "：数据重复，已过滤");
                        }
                    }
                }).print();


        env.execute();
    }
}
