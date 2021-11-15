package com.atguigu.day06.keystate;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

public class Flink02_KeyedState_ListState {
    public static void main(String[] args) throws Exception {
        // todo 针对每个传感器输出最高的3个水位值
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env
                .socketTextStream("localhost", 7777)
                .map(line -> {
                    String[] split = line.split(" ");
                    return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
                })
                .keyBy(k -> k.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private ListState<WaterSensor> listState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<WaterSensor>("listState", WaterSensor.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        listState.add(value);
                        // new一个集合 把liststate的数据装载进去，并排序
                        ArrayList<WaterSensor> list = new ArrayList<>();
                        Iterable<WaterSensor> iterable = listState.get();
                        for (WaterSensor waterSensor : iterable) {
                            list.add(waterSensor);
                        }
                        list.sort((c1,c2) -> c2.getVc()-c1.getVc());
                        /*list.sort(new Comparator<WaterSensor>() {
                            @Override
                            public int compare(WaterSensor o1, WaterSensor o2) {
                                return o2.getVc() - o1.getVc();
                            }
                        });*/
                        // 只需要前三个，一移除最后一个
                        if (list.size() > 3) {
                            list.remove(3);
                        }
                        listState.update(list);
                        out.collect(listState.get().toString());
                    }
                }).print();

        env.execute();
    }
}
