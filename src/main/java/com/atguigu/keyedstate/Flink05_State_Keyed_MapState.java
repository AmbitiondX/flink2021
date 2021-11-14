package com.atguigu.keyedstate;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink05_State_Keyed_MapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env
                .socketTextStream("localhost",7777)
                .map(value -> {
                    String[] split = value.split(" ");
                    return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    //定义状态 map中的key为水位因为要对相同的水位做去重，value为具体的数据
                    private MapState<Integer, WaterSensor> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, WaterSensor>("mapState",Integer.class,WaterSensor.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        //判断如果mapState中不包含相同的水位则输出
                        if (!mapState.contains(value.getVc())) {
                            out.collect(value);
                            // 更新状态
                            mapState.put(value.getVc(), value);
                        }
                    }
                }).print();
        env.execute();
    }
}
