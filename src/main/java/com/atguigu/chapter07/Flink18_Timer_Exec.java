package com.atguigu.chapter07;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink18_Timer_Exec {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env
                .socketTextStream("localhost", 7777)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(" ");
                        return new WaterSensor(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
                    }
                });

        // 监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流。
        SingleOutputStreamOperator<String> process = stream
                .keyBy(k -> k.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 用来保存定时器时间
                    private Long timer = Long.MIN_VALUE;
                    // 用来保存上一次的水位
                    private Integer lastVc = Integer.MIN_VALUE;

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 发送报警信息
                        ctx.output(new OutputTag<String>("output") {
                        }, "警告警告⚠️连续5s水位上升");
                        // 重置计时器
                        timer = Long.MIN_VALUE;
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        if (value.getVc() > lastVc) {
                            // 水位上升，注册定时器
                            if (timer == Long.MIN_VALUE) {
                                timer = System.currentTimeMillis() + 5000;
                                System.out.println("注册定时器" + timer);
                                ctx.timerService().registerProcessingTimeTimer(timer);
                            }
                        } else {
                            // 此时表示水位下降，需要重置定时器
                            System.out.println("删除定时器" + timer);
                            ctx.timerService().deleteProcessingTimeTimer(timer);
                            timer = Long.MIN_VALUE;
                        }
                        // 更新水位
                        lastVc = value.getVc();
                        out.collect(value.toString());
                    }
                });

        process.print("主流");
        process.getSideOutput(new OutputTag<String>("a ") {
        }).print("侧输出");


        env.execute();
    }
}
