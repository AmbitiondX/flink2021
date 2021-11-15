package com.atguigu.day06.keystate;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01_KeyedState_ValueState {
    public static void main(String[] args) throws Exception {
        // 检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env
                .socketTextStream("localhost", 7777)
                .map(line -> {
                    String[] split = line.split(" ");
                    return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
                })
                .keyBy(k -> k.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // todo 使用状态
                        // 1.从状态中获取上一次水位值
                        Integer lastVc = valueState.value() == null ? 0 : valueState.value();

                        // 2.拿当前水位跟上一次做比较
                        if (Math.abs(value.getVc() - lastVc) > 10) {
                            out.collect("警告：连续的两个水位线差值超过10");
                        }

                        // todo 更新状态
                        valueState.update(value.getVc());

                        /*if ((Math.abs(value.getVc() - (valueState.value() == null ? 0 : valueState.value())) > 10)) {
                            out.collect("警告：连续的两个水位线差值超过10");
                        }
                        valueState.update(value.getVc());*/
                    }
                }).print();

        env.execute();
    }
}
