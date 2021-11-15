package com.atguigu.day06.keystate;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.shaded.asm7.org.objectweb.asm.Type.INT;
import static org.apache.flink.shaded.asm7.org.objectweb.asm.Type.LONG;


public class Flink06_Timer_Exec_KeyedState {
    public static void main(String[] args) throws Exception {
        // todo 监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        KeyedStream<WaterSensor, String> keyedStream = env
                .socketTextStream("localhost", 7777)
                .map(line -> {
                    String[] split = line.split(" ");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                }))
                .keyBy(k -> k.getId());
        SingleOutputStreamOperator<String> process = keyedStream
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 保存上一次的水位信息
                    private ValueState<Integer> lastVc;
                    // 保存定时器时间
                    private ValueState<Long> timer;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVc", Integer.class, Integer.MIN_VALUE));
                        timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器启动
                        ctx.output(new OutputTag<String>("output") {
                        }, "警告！水位连续5s上升");
                        timer.clear();
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        if (value.getVc() > lastVc.value()) {
                            // 水位上涨，判断是否已经有定时器
                            if (timer.value() == null) {
                                // 定时器为空，需要设置定时器
                                timer.update(ctx.timestamp() + 5000 - 1);
                                System.out.println("设置定时器：" + (timer.value() + 1));
                                ctx.timerService().registerEventTimeTimer(timer.value());
                            }
                        } else {
                            // 水位下降，需要取消定时器
                            System.out.println("取消定时器：" + (timer.value() + 1));
                            if (timer.value() != null) {
                                ctx.timerService().deleteEventTimeTimer(timer.value());
                                timer.clear();
                            }
                        }
                        // 更新水位
                        lastVc.update(value.getVc());
                        // 输出信息
                        out.collect(value.toString());
                    }
                });

        process.print("主流");
        process.getSideOutput(new OutputTag<String>("output") {}).print("侧输出");


        env.execute();
    }
}
