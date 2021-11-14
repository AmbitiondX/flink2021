package com.atguigu.keyedstate;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink03_State_Keyed_ReducingState {
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
                    private ReducingState<WaterSensor> reducingState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>("reducing-state", new ReduceFunction<WaterSensor>() {
                            @Override
                            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                                return new WaterSensor(value1.getId(),value2.getTs(), value1.getVc()+ value2.getVc());
                            }
                        },WaterSensor.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        // 将当前数据聚合进状态
                        reducingState.add(value);
                        // 取出状态中的数据
                        WaterSensor waterSensor = reducingState.get();

                        out.collect(waterSensor);
                    }
                })
                .print();
        env.execute();
    }
}
