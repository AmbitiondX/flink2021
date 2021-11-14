package com.atguigu.keyedstate;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink06_State_Keyed_requestPlus {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 7777).map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] words = value.split(" ");
                return new WaterSensor(words[0], Long.parseLong(words[1]), Integer.parseInt(words[2]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs()*1000L ;
                            }
                        })
        );

        //3.按照传感器ID分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(date -> date.getId());

        //4.使用ProcessFunction实现5秒种水位不下降，则报警，且将报警信息输出到侧输出流
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            //a.定义状态
            private ValueState<Integer> vcState;
            private ValueState<Long> tsState;

            //b.初始化状态

            @Override
            public void open(Configuration parameters) throws Exception {
                vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vcState", Types.INT, Integer.MIN_VALUE));
                tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tsState", Types.LONG));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {


                //判断当前水位线是否高于上次水位线
                if (value.getVc() > vcState.value()) {
                    //判断定时器是否重置，是否为第一条数据
                    if (tsState.value() == null) {
                        System.out.println("waterMark="+ctx.timerService().currentWatermark());
                        System.out.println("注册定时器。。。"+(ctx.timestamp()+5000L));
                        //注册5秒钟之后的定时器
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000L);
                        tsState.update(ctx.timestamp()+5000L);
                    }
                } else {
                    //如果水位线没有上升则删除定时器
                    System.out.println("删除定时器。。。"+tsState.value());
                    ctx.timerService().deleteEventTimeTimer(tsState.value());
                    //重新注册5秒钟之后的定时器
                    System.out.println("重新注册定时器。。。"+(ctx.timestamp()+5000L));
                    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000L);
                    //将定时器的时间重置
                    tsState.clear();
                }
                //最后更新最新的水位线
                vcState.update(value.getVc());
                System.out.println("水位状态：="+vcState.value());
                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                System.out.println("删除定时器。。。" + tsState.value());
                tsState.clear();
                ctx.output(new OutputTag<String>("sideOut") {
                }, "传感器="+ctx.getCurrentKey() +"在ts="+timestamp+ "报警！！！！！！");
            }
        });

        result.print("主流");
        result.getSideOutput(new OutputTag<String>("sideOut") {
        }).print("报警信息");
        env.execute();
    }
}
