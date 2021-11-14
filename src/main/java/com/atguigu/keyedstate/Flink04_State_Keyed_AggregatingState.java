package com.atguigu.keyedstate;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink04_State_Keyed_AggregatingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env
                .socketTextStream("localhost",7777)
                .map(value -> {
                    String[] split = value.split(" ");
                    return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, Double>() {
                    private AggregatingState<Integer, Double> avgState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        avgState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer,Integer>, Double>("avgState", new AggregateFunction<Integer, Tuple2<Integer,Integer>, Double>() {
                            @Override
                            public Tuple2<Integer, Integer> createAccumulator() {
                                return Tuple2.of(0,0);
                            }

                            @Override
                            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                return Tuple2.of(accumulator.f0 + value,accumulator.f1 + 1);
                            }

                            @Override
                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                return accumulator.f0 * 1D / accumulator.f1;
                            }

                            @Override
                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                            }
                        }, Types.TUPLE(Types.INT,Types.INT)));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, Double>.Context ctx, Collector<Double> out) throws Exception {
                        avgState.add(value.getVc());

                        out.collect(avgState.get());
                    }
                }).print();
        env.execute();
    }
}
