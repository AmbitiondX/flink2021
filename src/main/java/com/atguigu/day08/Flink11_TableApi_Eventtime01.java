package com.atguigu.day08;

import com.atguigu.day02.source.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class Flink11_TableApi_Eventtime01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> andWatermarks = env.fromElements(
                        new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_2", 5000L, 40)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }));

        // 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从流中获取表
        // 用一个额外的字段作为时间的时间属性
//        Table waterSensorTable = tableEnv.fromDataStream(
//                andWatermarks,$("id"),$("ts"),$("vc"),$("et").rowtime());

        // 用已有的字段作为时间属性
        Table waterSensorTable = tableEnv.fromDataStream(andWatermarks, $("id"), $("ts").rowtime(), $("vc"));

        waterSensorTable.execute().print();

//        env.execute();
    }
}
