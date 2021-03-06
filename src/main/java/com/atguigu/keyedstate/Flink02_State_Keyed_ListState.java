package com.atguigu.keyedstate;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Flink02_State_Keyed_ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3);
        env
                .socketTextStream("localhost",7777)
                .map(value -> {
                    String[] split = value.split(" ");
                    return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, List<Integer>>() {
                    private ListState<Integer> vcState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        vcState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("vcState",Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, List<Integer>>.Context ctx, Collector<List<Integer>> out) throws Exception {
                        vcState.add(value.getVc());
                        // 获取状态中的所有水位高度，并排序
                        ArrayList<Integer> vcs = new ArrayList<>();
                        for (Integer vc : vcState.get()) {
                            vcs.add(vc);
                        }
                        // 降序排序
                        vcs.sort(((o1, o2) -> o2 - o1));
                        // 当长度超过3的时候 移除最后一个
                        if (vcs.size()>3){
                            vcs.remove(3);
                        }
                        vcState.update(vcs);
                        out.collect(vcs);
                    }
                })
                .print();
        env.execute();
    }
}
