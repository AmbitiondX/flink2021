package com.atguigu.chapter07;


import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Flink18_Timer_Exec_01 {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        SingleOutputStreamOperator<String> process = env
                .socketTextStream("localhost", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                }))
                .keyBy("id")
                //监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流。
                .process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
                    // 记录最近一次水位线
                    private Integer lastVc = Integer.MIN_VALUE;
                    // 记录定时器时间
                    private Long timer = Long.MIN_VALUE;

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Tuple, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        ctx.output(new OutputTag<String>("output") {
                        }, "⚠️警告，水位线连续5秒上升");
                        timer = Long.MIN_VALUE;
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        if (value.getVc() > lastVc) {
                            if (timer == Long.MIN_VALUE) {
                                // 此时设置定时器
                                timer = value.getTs() * 1000 + 5000;
                                System.out.println("设置定时器" + (timer - 1L));
                                ctx.timerService().registerEventTimeTimer((timer - 1L));
                            }
                        } else {
                            // 取消定时器
                            System.out.println("取消定时器" + (timer - 1L));
                            ctx.timerService().deleteEventTimeTimer((timer - 1L));
                            timer = Long.MIN_VALUE;
                        }
                        // 更新水位
                        lastVc = value.getVc();
                        out.collect(value.toString());
                    }
                });

        process.print("主流");
        process.getSideOutput(new OutputTag<String>("output"){}).print("侧输出");



//        env.getConfig().setAutoWatermarkInterval()
        env.execute();
    }
}
